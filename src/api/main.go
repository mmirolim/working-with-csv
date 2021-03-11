package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

/*
   Тестовое задание

В рамках данного задания требуется реализовать два сервиса, которые будут
взаимодействовать друг с другом. Задание должно быть реализовано на языке
программирования go, либо rust. Плюсом будут написанные юнит тесты (не
обязательная часть).
Первый сервис из двух будет реализовывать небольшой API, в основе которого будут
лежать три метода:
1. Первый метод будет читать CSV файл со списком компаний (название,
налоговый номер из 12 цифр, телефон, адрес, имя и фамилия директора) и
отдавать результат запроса в виде json массива. Файл необходимо составить
самостоятельно или оставить пустым.
2. Следующий метод будет давать возможность принять данные в запросе
(название компании, налоговый номер из 12 цифр, телефон, адрес, имя и
фамилия директора) и добавить запись в данный файл.
3. Последний метод будет давать возможность удалять строку из данного файла
по названию компании или налоговому номеру.
Необходимо подумать над конкурентно-выполняемыми методами (не обязательная
часть) и обеспечить их бесперебойную работу.
Второй сервис будет являться демоном, который будет периодически опрашивать
данное API раз в 20-30 секунд, вычитывать информацию о списке компаний и вести
актуальный список компаний в базе данных (postgres, mysql). В случае, если в списке
появилась новая компания, она должна быть добавлена в базу. Если же компания
стала отсутствовать в списке, то она должна быть удалена из базы данных.
Результат работы должен быть залит в репозиторий на github или его аналог.
Документация будет приветствоваться.
*/

var (
	storage *Storage
)

func main() {
	var err error
	// truncates file for test pugrposes
	storage, err = NewStorage("companies.csv", true)
	if err != nil {
		fmt.Println("OpenFile error", err.Error())
		os.Exit(1)
	}
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			select {
			case <-sigterm:
				fmt.Println("shutdown wait ...")
				storage.Close()
				os.Exit(0)
			}
		}

	}()

	http.HandleFunc("/list", ListCompaniesHandler)
	http.HandleFunc("/add", AddCompanyHandler)
	http.HandleFunc("/delete", DelCompanyHandler)

	log.Fatalln(http.ListenAndServe(":8080", nil))
}

const MaxFieldSize = 100
const RecordSize int64 = 12 + 100*4 + 5 // total length of chars in record

// properties has max lengths of 100 chars, inn has 12 chars
type Company struct {
	INN        string `json:"inn"`
	Name       string `json:"name"`
	Phone      string `json:"phone"`
	Address    string `json:"address"`
	Individual string `json:"individual"`
}

func (comp *Company) Validate() error {
	if len(comp.INN) != 12 {
		return errors.New("inn size should be 12 numbers")
	}
	for _, v := range []string{comp.Name, comp.Phone, comp.Address, comp.Individual} {
		if len(v) > MaxFieldSize {
			return errors.New("property max size 100 exceeded")
		}
	}
	return nil
}

func (comp *Company) CSVMarshal() ([]string, error) {
	if err := comp.Validate(); err != nil {
		return nil, err
	}
	data := make([]string, 5)
	data[0] = comp.INN
	// pads all fields
	for i, v := range []string{comp.Name, comp.Phone, comp.Address, comp.Individual} {
		d := MaxFieldSize - len(v)
		if d > 0 {
			v += strings.Repeat(" ", d)
		}
		data[i+1] = v
	}
	return data, nil
}

func (comp *Company) CSVUnmarshal(data []string) error {
	if len(data) != 5 {
		return fmt.Errorf("csv unmarshal error wrong number of fields %d, expected %d", len(data), 5)
	}
	comp.INN = data[0]

	comp.Name = strings.TrimRight(data[1], " ")
	comp.Phone = strings.TrimRight(data[2], " ")
	comp.Address = strings.TrimRight(data[3], " ")
	comp.Individual = strings.TrimRight(data[4], " ")

	return comp.Validate()
}

// by company inn and name
type index struct {
	ByINN  map[string]int64 // offset of start of a record
	ByName map[string]int64 // offset of start of a record
}

type Storage struct {
	mu        sync.Mutex
	file      io.ReadWriteSeeker
	index     index
	csvReader *csv.Reader
	csvWriter *csv.Writer
}

func NewStorage(fName string, truncate bool) (*Storage, error) {
	store := new(Storage)
	store.index.ByINN = map[string]int64{}
	store.index.ByName = map[string]int64{}

	// TODO should not truncate file
	fileMode := os.O_RDWR | os.O_CREATE
	if truncate {
		fileMode |= os.O_TRUNC
	} else {
		// TODO index file
	}

	f, err := os.OpenFile(fName, fileMode, 0666)
	if err != nil {
		return nil, err
	}
	store.file = f
	store.csvReader = csv.NewReader(f)
	store.csvWriter = csv.NewWriter(f)

	return store, nil
}

func (store *Storage) readAt(offset int64, whence int) (Company, error) {
	var comp Company

	offset, err := store.file.Seek(offset, whence)
	if err != nil {
		return comp, err
	}
	record, err := store.csvReader.Read()
	if err != nil {
		return comp, err
	}

	err = comp.CSVUnmarshal(record)

	return comp, err
}

func (store *Storage) writeAt(comp *Company, offset int64, whence int) error {
	data, err := comp.CSVMarshal()
	if err != nil {
		return err
	}

	_, err = store.file.Seek(offset, whence)
	if err != nil {
		return err
	}
	err = store.csvWriter.Write(data)
	if err != nil {
		return err
	}
	store.csvWriter.Flush()
	return nil
}

var ErrMissingID = errors.New("missing inn/name")
var ErrNotFound = errors.New("not found")

func (store *Storage) findCompanyOffset(comp *Company) (int64, error) {
	var offset int64
	var ok bool

	if len(comp.INN) > 0 {
		offset, ok = store.index.ByINN[comp.INN]
	} else if len(comp.Name) > 0 {
		offset, ok = store.index.ByName[comp.Name]
	} else {
		return offset, ErrMissingID
	}
	if !ok {
		return offset, ErrNotFound
	}
	return offset, nil
}

func (store *Storage) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.csvWriter.Flush()
	f := store.file.(*os.File)
	f.Sync()
	return f.Close()
}

func (store *Storage) AddCompany(comp *Company) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	offset, err := store.findCompanyOffset(comp)

	whence := io.SeekStart
	if err != nil {
		if err == ErrNotFound {
			// append to end of file
			offset = 0
			whence = io.SeekEnd
		} else {
			return err
		}
	}

	errWrite := store.writeAt(comp, offset, whence)
	if errWrite != nil {
		return errWrite
	}

	if err != nil && err == ErrNotFound {
		// add to index
		newOffset := RecordSize * int64(len(store.index.ByINN))
		store.index.ByINN[comp.INN] = newOffset
		store.index.ByName[comp.Name] = newOffset
	}
	return nil
}

func (store *Storage) DeleteCompany(comp *Company) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	offset, err := store.findCompanyOffset(comp)
	if err != nil {
		return err
	}

	// read last record
	compLast, err := store.readAt(-RecordSize, io.SeekEnd)
	if err != nil {
		return err
	}

	if comp.INN != compLast.INN {
		// overwrite with last record
		err = store.writeAt(&compLast, offset, io.SeekStart)
		if err != nil {
			return err
		}
		// update index
		store.index.ByINN[compLast.INN] = offset
		store.index.ByName[compLast.Name] = offset
	}
	// remove from index
	delete(store.index.ByINN, comp.INN)
	delete(store.index.ByName, comp.Name)

	// truncate file
	f := store.file.(*os.File)
	f.Seek(0, io.SeekStart) // set to beginning
	offsetLast := RecordSize * int64(len(store.index.ByINN))
	err = f.Truncate(offsetLast)
	return err
}

// TODO add pagination
func (store *Storage) ListCompanies() ([]Company, error) {
	var err error
	var record []string

	store.mu.Lock()
	defer store.mu.Unlock()

	// set cursor to start of file
	_, err = store.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	comps := make([]Company, 0, len(store.index.ByINN))
	var comp Company
	for {
		record, err = store.csvReader.Read()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			break
		}

		err = comp.CSVUnmarshal(record)
		if err != nil {
			break
		}
		comps = append(comps, comp)
	}

	return comps, err
}

func ListCompaniesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	comps, err := storage.ListCompanies()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data, err := json.Marshal(comps)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Write(data)
	return
}

func getCompanyFromReq(r *http.Request) (Company, error) {
	var comp Company
	if r.Method != http.MethodPost {
		return comp, errors.New("Wrong method")
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return comp, err
	}

	err = json.Unmarshal(data, &comp)
	return comp, err
}

func AddCompanyHandler(w http.ResponseWriter, r *http.Request) {
	comp, err := getCompanyFromReq(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = storage.AddCompany(&comp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func DelCompanyHandler(w http.ResponseWriter, r *http.Request) {
	comp, err := getCompanyFromReq(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = storage.DeleteCompany(&comp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

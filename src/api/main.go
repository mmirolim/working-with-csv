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
	"strings"
	"sync"
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
	mu           sync.RWMutex
	storageIndex Index
	storage      io.ReadWriteSeeker
	csvReader    *csv.Reader
	csvWriter    *csv.Writer
)

func main() {
	fmt.Println("csv file storage api")
	err := Setup("companies.csv", false)
	if err != nil {
		fmt.Println("OpenFile error", err.Error())
		os.Exit(1)
	}

	http.HandleFunc("/list", ListCompaniesHandler)
	http.HandleFunc("/add", AddCompanyHandler)
	http.HandleFunc("/delete", DelCompanyHandler)
	// TODO register handlers

	log.Fatalln(http.ListenAndServe(":8080", nil))
}

// TODO generate index if truncate false
func Setup(fName string, truncate bool) error {
	storageIndex.ByINN = map[string]int64{}
	storageIndex.ByName = map[string]int64{}

	// TODO should not truncate file
	fileMode := os.O_RDWR | os.O_CREATE
	if truncate {
		fileMode |= os.O_TRUNC
	} else {
		// TODO index file
	}

	f, err := os.OpenFile(fName, fileMode, 0666)
	if err != nil {
		return err
	}
	storage = f
	csvReader = csv.NewReader(storage)
	csvWriter = csv.NewWriter(storage)

	return nil
}

const MaxFieldSize = 100
const RecordSize int64 = 12 + 100*4 + 5 // total length of chars in record

// by company inn and name
type Index struct {
	NumRecords int64
	ByINN      map[string]int64 // offset of start of a record
	ByName     map[string]int64 // offset of start of a record
}

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

// TODO handle quit signal and panics?
func AddCompany(comp *Company) error {
	offset, ok := storageIndex.ByINN[comp.INN]
	// TODO move to storage api
	mu.Lock()
	defer mu.Unlock()
	if !ok {
		// append to end of file
		_, err := storage.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}
	} else {
		// update in previous position
		_, err := storage.Seek(offset, io.SeekStart)
		if err != nil {
			return err
		}
	}
	data, err := comp.CSVMarshal()
	if err != nil {
		return err
	}
	//	csvWriter := csv.NewWriter(storage)
	err = csvWriter.Write(data)
	if err != nil {
		return err
	}
	csvWriter.Flush()
	if !ok {
		// add to index
		newOffset := RecordSize * int64(len(storageIndex.ByINN))
		storageIndex.ByINN[comp.INN] = newOffset
		storageIndex.ByName[comp.Name] = newOffset
	}
	return err
}

func DeleteCompany(comp *Company) error {
	var offset int64
	var ok bool
	if len(comp.INN) > 0 {
		offset, ok = storageIndex.ByINN[comp.INN]
	} else if len(comp.Name) > 0 {
		offset, ok = storageIndex.ByName[comp.Name]
	} else {
		return errors.New("missing inn/name")
	}

	if !ok {
		// do nothing
		return nil
	}
	mu.Lock()
	defer mu.Unlock()

	// read last record
	offsetLast, err := storage.Seek(-RecordSize, io.SeekEnd)
	if err != nil {
		return err
	}
	record, err := csvReader.Read()
	if err != nil {
		return err
	}

	var compLast Company
	err = compLast.CSVUnmarshal(record)
	if err != nil {
		return err
	}

	// remove from index
	delete(storageIndex.ByINN, comp.INN)
	delete(storageIndex.ByName, comp.Name)
	// update index
	storageIndex.ByINN[compLast.INN] = offset
	storageIndex.ByName[compLast.Name] = offset

	// overwrite
	_, err = storage.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}

	err = csvWriter.Write(record)
	if err != nil {
		return err
	}
	csvWriter.Flush()
	// truncate file
	f := storage.(*os.File)
	f.Seek(0, io.SeekStart)      // set to beginning
	err = f.Truncate(offsetLast) //RecordSize * int64(len(storageIndex.ByINN)))
	return err
}

// TODO add pagination
func ListCompanies() ([]Company, error) {
	var comps []Company

	var err error
	var record []string
	mu.RLock()
	start := true
	defer func() {
		if r := recover(); r != nil && start {
			mu.RUnlock()
		}
	}()
	// set cursor to start of file
	_, err = storage.Seek(0, io.SeekStart)
	if err != nil {
		mu.RUnlock()
		return nil, err
	}
	for {
		record, err = csvReader.Read()
		if err == io.EOF {
			err = nil
			break
		}
		if err != nil {
			break
		}

		var comp Company
		err = comp.CSVUnmarshal(record)
		if err != nil {
			break
		}
		comps = append(comps, comp)
	}
	mu.RUnlock()
	start = false
	return comps, err
}

func ListCompaniesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	comps, err := ListCompanies()
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

func AddCompanyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var comp Company
	err = json.Unmarshal(data, &comp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = AddCompany(&comp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func DelCompanyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var comp Company
	err = json.Unmarshal(data, &comp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = DeleteCompany(&comp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	sampleData = `123456789000,Name123456789000                                                                                    ,Phone123456789000                                                                                   ,Address123456789000                                                                                 ,IndividualSomethingBigMore123456789000                                                              
123456789001,Name123456789001                                                                                    ,Phone123456789001                                                                                   ,Address123456789001                                                                                 ,IndividualSomethingBigMore123456789001                                                              
123456789002,Name123456789002                                                                                    ,Phone123456789002                                                                                   ,Address123456789002                                                                                 ,IndividualSomethingBigMore123456789002                                                              
123456789003,Name123456789003                                                                                    ,Phone123456789003                                                                                   ,Address123456789003                                                                                 ,IndividualSomethingBigMore123456789003                                                              
`
)

func TestReadAt(t *testing.T) {
	f, err := os.CreateTemp("", "*")
	assert.Nil(t, err, "CreateTemp failed")
	defer os.Remove(f.Name())

	_, err = f.Write([]byte(sampleData))
	assert.Nil(t, err, "file Write failed")
	f.Sync()

	storage, err := NewStorage(f.Name(), false)
	assert.Nil(t, err, "Setup failed")
	assert.NotNil(t, storage, "Storage is nil")

	// test build index
	var offsets []int64
	for _, v := range storage.index.ByName {
		offsets = append(offsets, v)
	}
	innByOffset := func(o int64) string {
		for k, v := range storage.index.ByINN {
			if v == o {
				return k
			}
		}
		return "NotFoundOffsetByInn"
	}
	// TODO test write
	for i := 0; i < 50; i++ {
		offset := offsets[rand.Intn(len(offsets))]
		comp, err := storage.readAt(offset, io.SeekStart)
		assert.Nil(t, err, "readAt failed")
		assert.Equal(t, comp.INN, innByOffset(offset))
	}
}

func TestNewStorage(t *testing.T) {
	f, err := os.CreateTemp("", "*")
	assert.Nil(t, err, "CreateTemp failed")
	defer os.Remove(f.Name())

	_, err = f.Write([]byte(sampleData))
	assert.Nil(t, err, "file Write failed")
	f.Sync()

	storage, err := NewStorage(f.Name(), false)
	assert.Nil(t, err, "Setup failed")
	assert.NotNil(t, storage, "Storage is nil")

	// test build index
	var offsets []int64
	for _, v := range storage.index.ByName {
		offsets = append(offsets, v)
	}

	for inn, offset := range storage.index.ByINN {
		comp, err := storage.readAt(offset, io.SeekStart)
		assert.Nil(t, err, "readAt failed")
		assert.Equal(t, comp.INN, inn, "wrong offset")
	}
	for name, offset := range storage.index.ByName {
		comp, err := storage.readAt(offset, io.SeekStart)
		assert.Nil(t, err, "readAt failed")
		assert.Equal(t, comp.Name, name, "wrong offset")
	}
}

func TestListCompanies(t *testing.T) {
	// write data
	f, err := os.CreateTemp("", "*")
	assert.Nil(t, err, "CreateTemp failed")
	defer os.Remove(f.Name())

	_, err = f.Write([]byte(sampleData))
	assert.Nil(t, err, "file Write failed")
	f.Sync()

	storage, err := NewStorage(f.Name(), false)
	assert.Nil(t, err, "Setup failed")

	comps, err := storage.ListCompanies()
	assert.Nil(t, err, "ListCompanies failed")

	ids := []string{
		"123456789000", "123456789001", "123456789002", "123456789003",
	}
	assert.Equal(t, len(ids), len(comps))
	var foundIds []string
	for i := range comps {
		foundIds = append(foundIds, comps[i].INN)
	}
	assert.ElementsMatch(t, ids, foundIds)
}

func TestAddCompany(t *testing.T) {
	storage, err := NewStorage("test-add-companies.csv", true)
	assert.Nil(t, err, "Setup failed")

	comps := generateNCompanies(20)

	for i := range comps {
		err = storage.AddCompany(&comps[i])
		assert.Nil(t, err, "AddCompany failed")
	}
	// update
	comps[1].Name = "NameChanged"
	err = storage.AddCompany(&comps[1])
	assert.Nil(t, err, "AddCompany failed")

	compsFound, err := storage.ListCompanies()
	assert.Nil(t, err, "ListCompanies")

	assert.ElementsMatch(t, comps, compsFound)
}

func TestDelCompany(t *testing.T) {
	storage, err := NewStorage("test-del-companies.csv", true)
	assert.Nil(t, err, "Setup failed")

	comps := generateNCompanies(20)
	for i := range comps {
		err = storage.AddCompany(&comps[i])
		assert.Nil(t, err, "AddCompany error")
	}

	// delete every even record
	var left []Company
	for i := range comps {
		if i%2 != 0 {
			left = append(left, comps[i])
			continue
		}
		err = storage.DeleteCompany(&comps[i])
		assert.Nil(t, err, "DeleteCompany error")
	}

	compsFound, err := storage.ListCompanies()
	assert.Nil(t, err, "ListCompanies")

	assert.ElementsMatch(t, left, compsFound)
}

// ~0.1 seconds, 100000 rps for 10 000 reqs
func TestTimeToAddCompanies10000(t *testing.T) {
	var err error
	storage, err = NewStorage("test-10000adds-companies.csv", true)
	assert.Nil(t, err, "Setup failed")

	comps := generateNCompanies(10000)

	handler := http.HandlerFunc(AddCompanyHandler)

	timeSpend := map[int]time.Duration{}
	start := time.Now()
	for i := range comps {
		payload, err := json.Marshal(comps[i])
		if err != nil {
			t.Error(err)
			return
		}
		rr := httptest.NewRecorder()
		req, err := http.NewRequest("POST", "/", bytes.NewBuffer(payload))
		if err != nil {
			t.Error(err)
			return
		}
		startNth := time.Now()
		handler.ServeHTTP(rr, req)
		timeSpend[i] = time.Since(startNth)
		// Check the status code is what we expect.
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, http.StatusOK)
			return
		}
	}
	fmt.Printf("Add %d companies took: %f s\n", len(comps), time.Since(start).Seconds())
	averageTime := 0.0
	for _, v := range timeSpend {
		averageTime += float64(v.Microseconds())
	}
	fmt.Printf("Average duration: %f microsecond\n ", averageTime/float64(len(timeSpend)))

}

// ~14 microseconds
func BenchmarkDelCompany(b *testing.B) {
	storage, err := NewStorage("test-del-companies.csv", true)
	assert.Nil(b, err, "Setup failed")

	comps := generateNCompanies(20)
	for i := range comps {
		err = storage.AddCompany(&comps[i])
		assert.Nil(b, err, "AddCompany error")
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err = storage.DeleteCompany(&comps[7])
		b.StopTimer()
		err = storage.AddCompany(&comps[7])
		assert.Nil(b, err, "AddCompany error")
		b.StartTimer()
	}
	assert.Nil(b, err)
}

// ~3 microseconds
func BenchmarkAddCompany(b *testing.B) {
	storage, err := NewStorage("test-add-companies.csv", true)
	assert.Nil(b, err, "Setup failed")

	comps := generateNCompanies(20)
	for i := range comps {
		err = storage.AddCompany(&comps[i])
		assert.Nil(b, err, "AddCompany error")
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err = storage.AddCompany(&comps[7])
	}
	assert.Nil(b, err)
}

// ~228 microseconds
func BenchmarkListCompany(b *testing.B) {
	storage, err := NewStorage("test-list-companies.csv", true)
	assert.Nil(b, err, "Setup failed")

	comps := generateNCompanies(100)
	for i := range comps {
		err = storage.AddCompany(&comps[i])
		assert.Nil(b, err, "AddCompany error")
	}
	var res []Company
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err = storage.ListCompanies()
	}
	assert.Nil(b, err)
	assert.Equal(b, len(comps), len(res))
}

func generateNCompanies(n int) []Company {
	var comps []Company

	for i := 0; i < n; i++ {
		id := 123456789000 + i
		comps = append(comps,
			Company{
				INN:        fmt.Sprintf("%d", id),
				Name:       fmt.Sprintf("Name%d", id),
				Phone:      fmt.Sprintf("Phone%d", id),
				Address:    fmt.Sprintf("Address%d", id),
				Individual: fmt.Sprintf("IndividualSomethingBigMore%d", id),
			})
	}
	return comps
}

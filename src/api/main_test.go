package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestListCompanies(t *testing.T) {
	// write data
	data := `123456789000,Name123456789000                                                                                    ,Phone123456789000                                                                                   ,Address123456789000                                                                                 ,IndividualSomethingBigMore123456789000                                                              
123456789001,Name123456789001                                                                                    ,Phone123456789001                                                                                   ,Address123456789001                                                                                 ,IndividualSomethingBigMore123456789001                                                              
123456789002,Name123456789002                                                                                    ,Phone123456789002                                                                                   ,Address123456789002                                                                                 ,IndividualSomethingBigMore123456789002                                                              
123456789003,Name123456789003                                                                                    ,Phone123456789003                                                                                   ,Address123456789003                                                                                 ,IndividualSomethingBigMore123456789003                                                              
`
	f, err := os.CreateTemp("", "*")
	assert.Nil(t, err, "CreateTemp failed")
	defer os.Remove(f.Name())

	_, err = f.Write([]byte(data))
	assert.Nil(t, err, "file Write failed")
	f.Sync()

	err = Setup(f.Name(), false)
	assert.Nil(t, err, "Setup failed")

	comps, err := ListCompanies()
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
	err := Setup("test-add-companies.csv", true)
	assert.Nil(t, err, "Setup failed")

	comps := generateNCompanies(20)

	for i := range comps {
		err = AddCompany(&comps[i])
		assert.Nil(t, err, "AddCompany failed")
	}
	// update
	comps[1].Name = "NameChanged"
	err = AddCompany(&comps[1])
	assert.Nil(t, err, "AddCompany failed")

	compsFound, err := ListCompanies()
	assert.Nil(t, err, "ListCompanies")

	assert.ElementsMatch(t, comps, compsFound)
}

func TestDelCompany(t *testing.T) {
	err := Setup("test-del-companies.csv", true)
	assert.Nil(t, err, "Setup failed")

	comps := generateNCompanies(20)
	for i := range comps {
		err = AddCompany(&comps[i])
		assert.Nil(t, err, "AddCompany error")
	}

	// delete every even record
	var left []Company
	for i := range comps {
		if i%2 != 0 {
			left = append(left, comps[i])
			continue
		}
		start := time.Now()
		err = DeleteCompany(&comps[i])
		fmt.Println("time del", time.Since(start))
		assert.Nil(t, err, "DeleteCompany error")
	}

	compsFound, err := ListCompanies()
	assert.Nil(t, err, "ListCompanies")

	assert.ElementsMatch(t, left, compsFound)
}

// ~14.5 microseconds
func BenchmarkDelCompany(b *testing.B) {
	err := Setup("test-del-companies.csv", true)
	assert.Nil(b, err, "Setup failed")

	comps := generateNCompanies(20)
	for i := range comps {
		err = AddCompany(&comps[i])
		assert.Nil(b, err, "AddCompany error")
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err = DeleteCompany(&comps[7])
		b.StopTimer()
		err = AddCompany(&comps[7])
		assert.Nil(b, err, "AddCompany error")
		b.StartTimer()
	}
	assert.Nil(b, err)
}

// ~3 microseconds
func BenchmarkAddCompany(b *testing.B) {
	err := Setup("test-add-companies.csv", true)
	assert.Nil(b, err, "Setup failed")

	comps := generateNCompanies(20)
	for i := range comps {
		err = AddCompany(&comps[i])
		assert.Nil(b, err, "AddCompany error")
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err = AddCompany(&comps[7])
	}
	assert.Nil(b, err)
}

// ~53 microseconds
func BenchmarkListCompany(b *testing.B) {
	err := Setup("test-list-companies.csv", true)
	assert.Nil(b, err, "Setup failed")

	comps := generateNCompanies(100)
	for i := range comps {
		err = AddCompany(&comps[i])
		assert.Nil(b, err, "AddCompany error")
	}
	var res []Company
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		res, err = ListCompanies()
	}
	assert.Nil(b, err)
	assert.Equal(b, len(comps), len(res))
}

func generateNCompanies(n int) []Company {
	var comps []Company

	for i := 0; i < 20; i++ {
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

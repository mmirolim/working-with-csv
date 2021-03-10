package main

import (
	"fmt"
	"testing"
)

// TODO check case editing with smaller values for last elements also
// TODO check delete and swap places with smaller strings

func TestAddCompany(t *testing.T) {
	err := Setup("test-add-companies.csv")
	if err != nil {
		t.Logf("Setup error %v", err)
		t.FailNow()
	}
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
	for i := range comps {
		err = AddCompany(&comps[i])
		if err != nil {
			t.Logf("AddCompany error %v", err)
			t.FailNow()
		}
	}
}

func TestDelCompany(t *testing.T) {
	err := Setup("test-del-companies.csv")
	if err != nil {
		t.Logf("Setup error %v", err)
		t.FailNow()
	}
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
	for i := range comps {
		err = AddCompany(&comps[i])
		if err != nil {
			t.Logf("AddCompany error %v", err)
			t.FailNow()
		}
	}

	// delete every even record
	for i := range comps {
		if i%2 != 0 {
			continue
		}
		if i > 10 {
			break
		}
		err = DeleteCompany(&comps[i])
		if err != nil {
			t.Logf("DelCompany %d error %v", i, err)
			t.FailNow()
		}
	}

	fmt.Println("Delete 17", &comps[17].INN)
	err = DeleteCompany(&comps[17])
	if err != nil {
		t.Logf("DelCompany error %v", err)
		t.FailNow()
	}

}

func TestListCompanies(t *testing.T) {
	err := Setup("test-list-companies.csv")
	if err != nil {
		t.Logf("Setup error %v", err)
		t.FailNow()
	}
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
	for i := range comps {
		err = AddCompany(&comps[i])
		if err != nil {
			t.Logf("AddCompany error %v", err)
			t.FailNow()
		}
	}

	comps, err = ListCompanies()
	if err != nil {
		t.Logf("List companies error %v", err)
		t.FailNow()
	}
	fmt.Println("num comps", len(comps))
	for i := range comps {
		fmt.Println(i, " ", comps[i])
	}
}

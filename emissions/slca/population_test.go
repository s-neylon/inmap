package slca

import (
	"context"
	"fmt"
	"github.com/evookelj/inmap/emissions/slca/eieio/ces"
	"os"
	"reflect"
	"testing"

	"github.com/BurntSushi/toml"

	"github.com/evookelj/inmap/emissions/slca/eieio/eieiorpc"
	"github.com/evookelj/inmap/epi"
)

// Set up directory location for configuration files.
func init() {
	os.Setenv("INMAP_ROOT_DIR", "../../")
}

// TODO: Test income and ethnicity differently
func TestPopulationCount(t *testing.T) {
	f, err := os.Open("testdata/test_config.toml")
	if err != nil {
		t.Fatal(err)
	}
	c := new(CSTConfig)
	if _, err = toml.DecodeReader(f, c); err != nil {
		t.Fatal(err)
	}
	if err = c.Setup(epi.NasariACS); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		year int32
		pop  []float64
	}{
		{
			year: 2001,
			pop:  []float64{50000, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			year: 2014,
			pop:  []float64{100000, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.year), func(t *testing.T) {
			p, err := c.populationIncomeCount(context.Background(), &eieiorpc.PopulationCountInput{
				Year: test.year, Population: "00-10%", AQM: "inmap"})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.pop, p) {
				t.Errorf("population: %v != %v", p, test.pop)
			}
		})
	}
}

func TestPopulationIncidence(t *testing.T) {
	f, err := os.Open("testdata/test_config.toml")
	if err != nil {
		t.Fatal(err)
	}
	c := new(CSTConfig)
	if _, err = toml.DecodeReader(f, c); err != nil {
		t.Fatal(err)
	}
	if err = c.Setup(epi.NasariACS); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		year int32
		pop  []float64
		io   []float64
	}{
		{
			year: 2000,
			pop:  []float64{50000, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			io:   []float64{399.9996241961794, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			year: 2001,
			pop:  []float64{50000, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			io:   []float64{399.9996241961794, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			year: 2002,
			pop:  []float64{50000*0.9 + 100000*0.1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			io:   []float64{439.9995866157974, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 400*0.9 + 800*0.1
		},
		{
			year: 2012,
			pop:  []float64{100000, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			io:   []float64{799.9992483923589, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			year: 2014,
			pop:  []float64{100000, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			io:   []float64{799.9992483923588, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			year: 2015,
			pop:  []float64{100000, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			io:   []float64{799.9992483923588, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprint(test.year), func(t *testing.T) {
			p, err := c.PopulationIncidence(context.Background(), &eieiorpc.PopulationIncidenceInput{
				Year: test.year, Population: "TotalPop", HR: "NasariACS", AQM: "inmap"})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(test.pop, p.Population) {
				t.Errorf("population: %v != %v", p.Population, test.pop)
			}
			if !reflect.DeepEqual(test.io, p.Incidence) {
				t.Errorf("incidence: %v != %v", p.Incidence, test.io)
			}
		})
	}

	t.Run("Demograph wrapper", func (t *testing.T) {
			dem := ces.EthnicityToDemograph(eieiorpc.Ethnicity_Black)
			test := tests[0]
			pDem, err := c.PopulationIncidenceDem(context.Background(), &eieiorpc.PopulationIncidenceDemInput{
				Year:       test.year,
				Population: dem,
				HR:         "NasariACS",
				AQM:        "inmap",
			})
			if err != nil {
				t.Fatal(err)
			}
			pNormal, err := c.PopulationIncidence(context.Background(), &eieiorpc.PopulationIncidenceInput{
				Year:       test.year,
				Population: "Black",
				HR:         "NasariACS",
				AQM:        "inmap",
			})
			if err != nil {
				t.Fatal(err)
			}

		if !reflect.DeepEqual(pNormal.Population, pDem.Population) {
			t.Errorf("population: %v != %v", pNormal.Population, pDem.Population)
		}
		if !reflect.DeepEqual(pNormal.Incidence, pDem.Incidence) {
			t.Errorf("incidence: %v != %v", pNormal.Incidence, pDem.Incidence)
		}

	})
}

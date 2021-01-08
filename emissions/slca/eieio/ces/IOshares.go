/*
Copyright © 2017 the InMAP authors.
This file is part of InMAP.

InMAP is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

InMAP is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with InMAP.  If not, see <http://www.gnu.org/licenses/>.*/

// Package ces translates Consumer Expenditure Survey (CES) demographic data
// to EIO categories.
package ces

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/evookelj/inmap/emissions/slca/eieio/eieiorpc"
	"github.com/gonum/floats"

	"github.com/tealeg/xlsx"
)

// CES holds the fractions of personal expenditures that are incurred by
// non-hispanic white people by year and 389-sector EIO category.
type CES struct {
	// StartYear and EndYear are the beginning and ending
	// years for data availability, respectively.
	StartYear, EndYear int

	// fraction of total consumption incurred by demographics
	// in each year and IO sector
	ethnicityFractions map[eieiorpc.Ethnicity]map[int]map[string]float64
	decileFractions map[eieiorpc.Decile]map[int]map[string]float64

	eio eieiorpc.EIEIOrpcServer
}

// txtToSlice converts a line-delimited list of strings in
// a text file into a slice.
func txtToSlice(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

// matchSharesToSectors takes two maps:
// 	1. Map of each IO sector to one or more CE sectors that it consists of
// 	2. Map of each CE sector to the a) spending share and b) aggregate spending
// 		associated with the desired demographic group
//
// It returns a new map is created that matches each IO sector to a slice of one
// or more sets of spending shares and aggregate spending amounts from the
// relevant CE sectors.
func matchSharesToSectors(m map[string][]string, m2 map[string][]float64) (m3 map[string][]float64) {
	m3 = make(map[string][]float64)
	for key, vals := range m {
		for _, mVal := range vals {
			for _, m2Val := range m2[mVal] {
				m3[key] = append(m3[key], m2Val)
			}
		}
	}
	return m3
}

// weightedAvgShares takes the map created by matchSharesToSectors()
// and performs a weighted average (based on aggregate spending) of all the
// shares that are associated with each IO code.
func weightedAvgShares(m map[string][]float64) (m2 map[string]float64) {
	m2 = make(map[string]float64)
	for key, vals := range m {
		var aggregateSum float64
		for i, val := range vals {
			if (i+1)%2 != 0 {
				aggregateSum = aggregateSum + val
			}
		}
		var numerator float64
		for i := 0; i < len(vals)/2; i++ {
			numerator = numerator + vals[i*2]*vals[i*2+1]
		}
		if aggregateSum != 0 {
			m2[key] = numerator / aggregateSum
		} else {
			m2[key] = 0
		}
	}
	return m2
}

// dataToXlsxFile populates the output tables with spending shares
func dataToXlsxFile(d map[string]float64, xlsxFile *xlsx.File, sheet string) {
	var cell *xlsx.Cell
	for i, row := range xlsxFile.Sheet[sheet].Rows {
		// Skips top row which contains column headings
		if i == 0 {
			continue
		}
		cell = row.AddCell()
		cell.SetFloat(d[row.Cells[0].Value])
	}
}

// NewCES loads data into a new CES object.
func NewCES(eio eieiorpc.EIEIOrpcServer, dataDir string) (*CES, error) {
	dataDir = os.ExpandEnv(dataDir)
	// Create map of IO categories to CE categories
	ioCEMap := make(map[string][]string)
	ioCEXLSXPath := filepath.Join(dataDir, "IO-CEcrosswalk.xlsx")
	ioCEXLSX, err := xlsx.OpenFile(ioCEXLSXPath)
	if err != nil {
		return nil, err
	}

	const NumDeciles = 10
	const StartYear = 2003
	const EndYear = 2015
	ces := CES{
		StartYear:       StartYear,
		EndYear:         EndYear,
		ethnicityFractions: make(map[eieiorpc.Ethnicity]map[int]map[string]float64),
		decileFractions: make(map[eieiorpc.Decile]map[int]map[string]float64),
		eio:             eio,
	}

	for _, val := range eieiorpc.Ethnicity_value {
		dem := eieiorpc.Ethnicity(val)
		if dem != eieiorpc.Ethnicity_Ethnicity_All {
			ces.ethnicityFractions[dem] = make(map[int]map[string]float64)
		}
	}
	for _, val := range eieiorpc.Decile_value {
		dem := eieiorpc.Decile(val)
		if dem != eieiorpc.Decile_Decile_All {
			ces.decileFractions[dem] = make(map[int]map[string]float64)
		}
	}

	for _, sheet := range ioCEXLSX.Sheets {
		for i, row := range sheet.Rows {
			// Skip column headers
			if i == 0 {
				continue
			}
			key := row.Cells[0].Value // The key is the IO commodity
			for j := 1; j < len(row.Cells); j++ {
				if row.Cells[j].Value != "" {
					ioCEMap[key] = append(ioCEMap[key], row.Cells[j].Value)
				}
			}
		}
	}

	ceKeys, err := txtToSlice(filepath.Join(dataDir, "CEkeys.txt"))
	if err != nil {
		return nil, err
	}

	// hardcoded: which column corresponds to which group?
	const EthnicityAggregateCol = 1
	const NonHispanicWhiteCol = 4
	const LatinoCol = 2
	const BlackCol = 5
	const DecileAggregateCol = 1

	// Loop through each year of available data
	// - data contain necessary metrics starting in 2003
	for year := ces.StartYear; year <= ces.EndYear; year++ {

		// BY ETHNICITY GROUP
		demCols := []int{BlackCol, LatinoCol, NonHispanicWhiteCol}
		ethnicityInputFileName := filepath.Join(dataDir, fmt.Sprintf("hispanic%d.xlsx", year))
		ethnicityRes, err := getDataForDemographics(ethnicityInputFileName, ceKeys, ioCEMap, EthnicityAggregateCol, demCols)
		if err != nil {
			return nil, err
		}
		ces.ethnicityFractions[eieiorpc.Ethnicity_Black][year] = ethnicityRes[0]
		ces.ethnicityFractions[eieiorpc.Ethnicity_Hispanic][year] = ethnicityRes[1]
		ces.ethnicityFractions[eieiorpc.Ethnicity_WhiteOther][year] = ethnicityRes[2]

		// BY INCOME DECILE
		if year >= 2014 { // decile data DNE earlier than this
			decileCols := make([]int, NumDeciles)
			for idx := 0; idx < NumDeciles; idx++ {
				decileCols[idx] = 2 + idx // lowest 10% at 2, then second 10% at 3, so on ...
			}
			decileInputFileName := filepath.Join(dataDir, fmt.Sprintf("decile%d.xlsx", year))
			decileResults, err := getDataForDemographics(decileInputFileName, ceKeys, ioCEMap, DecileAggregateCol, decileCols)
			if err != nil {
				return nil, err
			}

			for decileIdx, decileResult := range decileResults {
				// NOTE: This relies on protobuf enum values being
				// lowest 10% = 0, second 10% = 1, ...
				decile := eieiorpc.Decile(decileIdx)
				ces.decileFractions[decile][year] = decileResult
			}
		}
	}
	ces.normalize()
	return &ces, nil
}

func EthnicityToDemograph(eth eieiorpc.Ethnicity) *eieiorpc.Demograph {
	return &eieiorpc.Demograph{
		Demographic: &eieiorpc.Demograph_Ethnicity{
			eth,
		},
	}
}

func DecileToDemograph(dec eieiorpc.Decile) *eieiorpc.Demograph {
	return &eieiorpc.Demograph{
		Demographic: &eieiorpc.Demograph_Decile{
			dec,
		},
	}
}


// Returns IO data for demographics in the order they are provided
func getDataForDemographics(inputFileName string, ceKeys []string, ioCEMap map[string][]string, AggregateCol int, demCols []int) ([]map[string]float64, error) {
	// Flags specific string values to be replaced when looping through data
	// a Value is too small to display.
	// b Data are likely to have large sampling errors.
	// c No data reported.
	r := strings.NewReplacer("a/", "", "b/", "", "c/", "", " ", "")
	s2f := func(s string) (float64, error) {
		s2 := r.Replace(s)
		if s2 == "" {
			return 0, nil
		}
		return strconv.ParseFloat(s2, 64)
	}

	// Open raw CE data files
	inputFile, err := xlsx.OpenFile(inputFileName)
	if err != nil {
		return nil, err
	}
	cesSheet := inputFile.Sheets[0]

	dataMapCES := make([]map[string][]float64, 0, len(demCols))
	for range demCols {
		dataMapCES = append(dataMapCES, make(map[string][]float64))
	}

	for rowIdx, row := range cesSheet.Rows {

		// Skip blank rows
		if len(row.Cells) == 0 {
			continue
		}

		// The key is the CES category.
		key := strings.Trim(row.Cells[0].Value, " ")

		// For each CE category that we are interested in, find the
		// corresponding row in the raw CE data files and pull spending
		// share and aggregate spending numbers.
		for _, line := range ceKeys {
			match, err := regexp.MatchString("^"+line+"$", key)
			if err != nil {
				return nil, err
			}
			if match {

				var aggregate float64
				var meanExpenditureByDem = make([]float64, len(demCols))

				/*For each ethnicity group and product category, data offers
				share of consumption of that good incurred by that group

				Whereas for each decile and product category, data offers
				mean expenditure in $, and share of that decile's expenditure
				that goes towards that good. So we have to translate it
				into data more like that of ethnicity
				*/
				if strings.Contains(inputFileName, "decile") {
					const MeanOffset = 1
					rowOfMeans := cesSheet.Rows[rowIdx+MeanOffset]
					for demIdx, colNum := range demCols {
						demValue, err := s2f(rowOfMeans.Cells[colNum].Value)
						if err != nil {
							return nil, err
						}
						aggregate += demValue
						meanExpenditureByDem[demIdx] = demValue
					}
				} else {
					var err error
					aggregate, err = s2f(row.Cells[AggregateCol].Value)
					if err != nil {
						return nil, err
					}

					for demIdx, colNum := range demCols {
						colShare, err := s2f(row.Cells[colNum].Value)
						if err != nil {
							return nil, err
						}
						meanExpenditureByDem[demIdx] = aggregate * colShare / 100
					}
				}

				for demIdx, meanExpenditure := range meanExpenditureByDem {
					dataMapCES[demIdx][key] = append(dataMapCES[demIdx][key], meanExpenditure)
					dataMapCES[demIdx][key] = append(dataMapCES[demIdx][key], meanExpenditure / aggregate)
				}
			}
		}
	}

	finalResultsByDem := make([]map[string]float64, 0, len(dataMapCES))
	for _, demCESDataMap := range dataMapCES {
		demIO := matchSharesToSectors(ioCEMap, demCESDataMap)
		demFinal := weightedAvgShares(demIO)
		finalResultsByDem = append(finalResultsByDem, demFinal)
	}
	return finalResultsByDem, nil
}

func (ces *CES) normalize() {
	for year := range ces.ethnicityFractions[eieiorpc.Ethnicity_WhiteOther] {
		for sector := range ces.ethnicityFractions[eieiorpc.Ethnicity_WhiteOther][year] {
			totalForSectorEthnicity := float64(0)
			for dem, _ := range ces.ethnicityFractions {
				totalForSectorEthnicity += ces.ethnicityFractions[dem][year][sector]
			}
			for dem, _ := range ces.ethnicityFractions {
				ces.ethnicityFractions[dem][year][sector] /= totalForSectorEthnicity
			}
		}

		for sector := range ces.decileFractions[eieiorpc.Decile_Decile_All][year] {
			totalForSectorDeciles := float64(0)
			for dem, _ := range ces.decileFractions {
				totalForSectorDeciles += ces.decileFractions[dem][year][sector]
			}
			for dem, _ := range ces.decileFractions {
				ces.decileFractions[dem][year][sector] /= totalForSectorDeciles
			}
		}
	}
}

// ErrMissingSector happens when a IO sector is requested which there is
// no data for.
type ErrMissingSector struct {
	sector string
	year   int
}

func (e ErrMissingSector) Error() string {
	return fmt.Sprintf("ces: missing IO sector '%s'; year %d", e.sector, e.year)
}

func (c *CES) getFrac(dem eieiorpc.Demograph) func(int, string)(float64, error) {
	var ethnicity eieiorpc.Ethnicity
	var decile eieiorpc.Decile
	isEthnicity, isDecile := false, false
	switch typedDem := dem.Demographic.(type) {
		case *eieiorpc.Demograph_Ethnicity:
			ethnicity, isEthnicity = typedDem.Ethnicity, true
		case *eieiorpc.Demograph_Decile:
			decile, isDecile = typedDem.Decile, true
	}

	var catchAll = func(int, string) (float64, error) { return 1, nil }

	if (isEthnicity && ethnicity == eieiorpc.Ethnicity_Ethnicity_All) ||
		(isDecile && decile == eieiorpc.Decile_Decile_All) {
		return catchAll
	}

	var mapForDemograph map[int]map[string]float64
	var mapOk bool
	if isEthnicity {
		mapForDemograph, mapOk = c.ethnicityFractions[ethnicity]
	} else if isDecile {
		mapForDemograph, mapOk = c.decileFractions[decile]
	}
	return func(year int, IOSector string) (float64, error) {
		if year > c.EndYear || year < c.StartYear {
			return math.NaN(), fmt.Errorf("ces: year %d is outside of allowed range %d--%d", year, c.StartYear, c.EndYear)
		}
		if !mapOk {
			return math.NaN(), fmt.Errorf("invalid demograph: %s", dem.String())
		}

		v, ok := mapForDemograph[year][IOSector]
		if !ok {
			return math.NaN(), ErrMissingSector{sector: IOSector, year: year}
		}
		return v, nil
	}
}


// DemographicConsumption returns domestic personal consumption final demand
// plus private final demand for the specified demograph.
// Personal consumption and private residential expenditures are directly adjusted
// using the getFrac function.
// Other private expenditures are adjusted by the scalar:
//		adj = sum(getFrac(personal + private_residential)) / sum(personal + private_residential)
// Acceptable demographs:
//		Black: People self-identifying as black or African-American.
//		Hispanic: People self-identifying as Hispanic or Latino.
//		WhiteOther: People self identifying as white or other races besides black, and not Hispanic.
//		All: The total population.
func (c *CES) DemographicConsumption(ctx context.Context, in *eieiorpc.DemographicConsumptionInput) (*eieiorpc.Vector, error) {
	return c.adjustDemand(ctx, in.EndUseMask, in.Year, c.getFrac(*in.Demograph))
}

// adjustDemand returns domestic personal consumption final demand plus private final demand
// after adjusting it using the getFrac function.
// Personal consumption and private residential expenditures are directly adjusted
// using the getFrac function.
// Other private expenditures are adjusted by the scalar:
//		adj = sum(getFrac(personal + private_residential)) / sum(personal + private_residential)
func (c *CES) adjustDemand(ctx context.Context, commodities *eieiorpc.Mask, year int32, frac func(year int, commodity string) (float64, error)) (*eieiorpc.Vector, error) {
	// First, get the adjusted personal consumption.
	pc, err := c.eio.FinalDemand(ctx, &eieiorpc.FinalDemandInput{
		FinalDemandType: eieiorpc.FinalDemandType_PersonalConsumption,
		Year:            year,
		Location:        eieiorpc.Location_Domestic,
	})
	if err != nil {
		return nil, err
	}

	// Then, get the private residential expenditures.
	pcRes, err := c.eio.FinalDemand(ctx, &eieiorpc.FinalDemandInput{
		FinalDemandType: eieiorpc.FinalDemandType_PrivateResidential,
		Year:            year,
		Location:        eieiorpc.Location_Domestic,
	})
	if err != nil {
		return nil, err
	}

	// Now, add the two together
	floats.Add(pc.Data, pcRes.Data)

	// Next, adjust the personal consumption by the provided fractions.
	demand := &eieiorpc.Vector{
		Data: make([]float64, len(pc.Data)),
	}
	commodityList, err := c.eio.Commodities(ctx, nil)
	if err != nil {
		return nil, err
	}
	for i, sector := range commodityList.List {
		v := pc.Data[i]
		if v == 0 {
			continue
		}
		f, err := frac(int(year), sector)
		if err != nil {
			return nil, err
		}
		demand.Data[i] = v * f
	}

	// Now we create an adjustment factor and use it to adjust the
	// rest of the private expenditures.
	adj := floats.Sum(demand.Data) / floats.Sum(pc.Data)
	for _, dt := range []eieiorpc.FinalDemandType{
		eieiorpc.FinalDemandType_PrivateStructures,
		eieiorpc.FinalDemandType_PrivateEquipment,
		eieiorpc.FinalDemandType_PrivateIP,
		eieiorpc.FinalDemandType_InventoryChange} {

		d, err := c.eio.FinalDemand(ctx, &eieiorpc.FinalDemandInput{
			FinalDemandType: dt,
			Year:            year,
			Location:        eieiorpc.Location_Domestic,
		})
		if err != nil {
			return nil, err
		}
		floats.AddScaled(demand.Data, adj, d.Data)
	}
	if commodities != nil {
		floats.Mul(demand.Data, commodities.Data) // Apply the mask.
	}
	return demand, nil
}

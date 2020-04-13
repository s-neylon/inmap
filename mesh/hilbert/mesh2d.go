/*
Copyright © 2020 the InMAP authors.
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
along with InMAP.  If not, see <http://www.gnu.org/licenses/>.
*/

package hilbert

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/spatialmodel/inmap/v2/mesh"
	"github.com/spatialmodel/inmap/v2/plot"
	"github.com/spatialmodel/inmap/v2/unit"

	"github.com/golang/geo/r2"
	"github.com/golang/geo/s2"
)

// EarthRadius is the radius of the Earth at the equator.
const EarthRadius = 6.3781e6 // meters

// Make sure our mesh fulfills the interface.
var _ mesh.Mesh = &Mesh2D{}

// Mesh2D represents a 2D quasi-rectangular mesh.
type Mesh2D struct {
	cells         []s2.CellID
	boundaryCells []s2.CellID
	faces         []*face1D
}

// NewMesh2D returns a new 2D mesh at the specified resolution level,
// approximately covering the area specified by b.
// Information regarding resolution levels is available at
// https://s2geometry.io/resources/s2cell_statistics.html.
func NewMesh2D(b Bounds, level int) *Mesh2D {
	rc := &s2.RegionCoverer{
		MinLevel: level,
		MaxLevel: level,
	}
	m := &Mesh2D{
		cells: rc.Covering(b),
	}
	m.build()
	return m
}

// build creates the mesh faces and indexes.
func (m *Mesh2D) build() {
	// Add cells to index for lookup.
	index := make(map[s2.CellID]int)
	for i, c := range m.cells {
		index[c] = i
	}
	m.faces = make([]*face1D, 0)

	for i, c := range m.cells {
		nb := c.EdgeNeighbors()
		for k, nbc := range nb {
			// j is the index of the neighber.
			// If j is not in the index, then the neighbor is a boundary condition.
			// If j < i, that means that we've already processed this face.
			j, ok := index[nbc]
			if !ok {
				m.boundaryCells = append(m.boundaryCells, nbc)
			} else if j < i {
				continue
			}
			c2 := s2.CellFromCellID(c)
			var f *face1D
			switch k {
			case 0: // down
				f = &face1D{Edge: s2.Edge{V0: c2.Vertex(0), V1: c2.Vertex(1)}, lesser: nbc, greater: c}
			case 1: // right
				f = &face1D{Edge: s2.Edge{V0: c2.Vertex(1), V1: c2.Vertex(2)}, lesser: c, greater: nbc}
			case 2: // up
				f = &face1D{Edge: s2.Edge{V0: c2.Vertex(2), V1: c2.Vertex(3)}, lesser: c, greater: nbc}
			case 3: // left
				f = &face1D{Edge: s2.Edge{V0: c2.Vertex(3), V1: c2.Vertex(0)}, lesser: nbc, greater: c}
			default:
				panic("not possible")
			}
			m.faces = append(m.faces, f)
		}
	}
}

// Dims returns that this mesh is 2D.
func (m *Mesh2D) Dims() int { return 2 }

// Cells returns the number of cells in this mesh.
func (m *Mesh2D) Cells() int { return len(m.cells) }

// Cell returns the cell at the given index (where i < Cells())
func (m *Mesh2D) Cell(i int) mesh.Cell { return cell2D(m.cells[i]) }

// Faces returns the total number of faces in the mesh
func (m *Mesh2D) Faces() int { return len(m.faces) }

// Face returns the face at the given index, where i < Faces().
// The face has 1 dimension.
func (m *Mesh2D) Face(i int) mesh.Face { return m.faces[i] }

type cell2D s2.CellID

// Measure returns the characteristic measure of the face.
// Use the Units method to get the units of the measure.
func (c cell2D) Measure() float64 {
	return s2.CellFromCellID(s2.CellID(c)).ApproxArea() * EarthRadius * EarthRadius
}

// Unit returns the units of the characteristic measure.
func (c cell2D) Unit() unit.Unit { return unit.MeterSquared }

type face1D struct {
	s2.Edge
	lesser, greater s2.CellID
}

// Lesser returns the cell that is on the lesser side
// of this face (the side that has a lower value in whatever
// coordinate system is being used).
func (f *face1D) Lesser() mesh.Cell { return cell2D(f.lesser) }

// Greater returns the cell that is
// on the greater side of this face.
func (f *face1D) Greater() mesh.Cell { return cell2D(f.greater) }

// Measure returns returns the length of the face in meters.
func (f *face1D) Measure() float64 {
	v0 := s2.LatLngFromPoint(f.Edge.V0)
	v1 := s2.LatLngFromPoint(f.Edge.V1)
	return v0.Distance(v1).Radians() * EarthRadius
}

// Unit returns meters, the units of the face measure.
func (f *face1D) Unit() unit.Unit { return unit.Meter }

// PlotCells returns the polygons that make up the mesh cells
// for plotting with the given projection.
func (m *Mesh2D) PlotCells(p Projection) []plot.XYs {
	return m.plotCells(p, m.cells)
}

// PlotBoundaryCells returns the polygons that make up the mesh boundary cells
// for plotting with the given projection.
func (m *Mesh2D) PlotBoundaryCells(p Projection) []plot.XYs {
	return m.plotCells(p, m.boundaryCells)
}

func (m *Mesh2D) plotCells(p Projection, cells []s2.CellID) []plot.XYs {
	e := s2.NewEdgeTessellator(p, 1.0e-5)
	o := make([]plot.XYs, len(cells))
	for i, c := range cells {
		vs2 := s2.LoopFromCell(s2.CellFromCellID(c)).Vertices()
		var v []r2.Point
		v = e.AppendProjected(vs2[0], vs2[1], v)
		v = e.AppendProjected(vs2[1], vs2[2], v)
		v = e.AppendProjected(vs2[2], vs2[3], v)
		v = e.AppendProjected(vs2[3], vs2[0], v)
		ixy := make(plot.XYs, len(v))
		for j, vj := range v {
			ixy[j].X = vj.X
			ixy[j].Y = vj.Y
		}
		o[i] = ixy
	}
	return o
}

// PlotEdges returns the lines that make up the mesh edges
// for plotting with the given projection.
func (m *Mesh2D) PlotEdges(p Projection) []plot.XYs {
	e := s2.NewEdgeTessellator(p, 1.0e-5)
	o := make([]plot.XYs, len(m.faces))
	for i, f := range m.faces {
		var v []r2.Point
		v = e.AppendProjected(f.Edge.V0, f.Edge.V1, v)
		ixy := make(plot.XYs, len(v))
		for j, vj := range v {
			ixy[j].X = vj.X
			ixy[j].Y = vj.Y
		}
		o[i] = ixy
	}
	return o
}

// MarshalBinary serializes this mesh into a byte array.
func (m *Mesh2D) MarshalBinary() []byte {
	b := bytes.NewBuffer(nil)
	if err := binary.Write(b, binary.LittleEndian, m.cells); err != nil {
		panic(err)
	}
	return b.Bytes()
}

// UnmarshalBinary initializes this mesh from a byte array.
func (m *Mesh2D) UnmarshalBinary(b []byte) error {
	r := bytes.NewReader(b)
	for {
		var v s2.CellID
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("unmarshalling mesh: %w", err)
		}
		m.cells = append(m.cells, v)
	}
	m.build()
	return nil
}

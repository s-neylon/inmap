/*
Copyright © 2019 the InMAP authors.
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

// Package mesh defines interfaces for spatial meshes.
package mesh

import (
	"github.com/spatialmodel/inmap/v2/unit"
)

// Mesh describes a spatial mesh.
type Mesh interface {
	// Dims returns the number of spatial dimensions
	// in this mesh.
	Dims() int

	// Cells is the total number of cells in this Mesh.
	Cells() int

	// Cell returns the cell at the given index (where i < Cells())
	Cell(i int) Cell

	// Faces returns the number of faces that comprise
	// the geometry of the cell at the given index (where cell < Cells()).
	Faces() int

	// Face returns the face at the given cell index (where cell < Cells())
	// and face index (where face < Faces(cell)).
	// The face will have one fewer dimensions than the cell.
	Face(i int) Face

	// MarshalBinary serializes this mesh into a byte array.
	MarshalBinary() []byte

	// UnmarshalBinary initializes this mesh from a byte array.
	UnmarshalBinary([]byte) error
}

// AdaptiveMesh specifies a mesh with cells that can
// conditionally divide or combine.
type AdaptiveMesh interface {
	Mesh

	// Adapt mutates the grid.
	Adapt(AdaptFunc)
}

// Meshes describes a group of meshes.
type Meshes interface {
	Mesh

	// Meshes returns the number of meshes
	Meshes() int

	// At returns the i'th mesh.
	At(i int) Mesh
}

// AdaptFunc determines wither the mesh cell at the
// specified index should be split into multiple cells,
// combined with the surrounding cells, or neither.
type AdaptFunc func(int) SplitOrCombine

// SplitOrCombine specifies whether a grid cell should be
// split, combined with its neighbors, or left alone.
type SplitOrCombine int

const (
	// Neither specifies that the cell should be left alone.
	Neither SplitOrCombine = iota
	// Split specifies that the cell should be split.
	Split
	// Combine specifies that the cell should be
	// combined with its neighbors.
	Combine
)

// Cell specifies a cell in a mesh
type Cell interface {
	// Measure returns the characteristic measure of the face.
	// Use the Units method to get the units of the measure.
	Measure() float64

	// Unit returns the units of the characteristic measure.
	Unit() unit.Unit
}

// Face represents the face of a cell.
type Face interface {
	// Lesser returns the cell that is on the lesser side
	// of this face (the side that has a lower value in whatever
	// coordinate system is being used).
	Lesser() Cell

	// Greater returns the cell that is
	// on the greater side of this face.
	Greater() Cell

	// Measure returns the characteristic measure of the face.
	// Use the Units method to get the units of the measure.
	Measure() float64

	// Unit returns the units of the characteristic measure.
	Unit() unit.Unit
}

// FaceOpposing specifies a face that has another face opposing
// it accross a given cell.
type FaceOpposing interface {
	Face

	// Opposing returns the Face directly across from the
	// receiver in the given cell.
	Opposing() Face
}

// Point represents a point in vector space.
type Point interface {
	// Len returns the number of dimensions of this point.
	Len() int

	// V returns the point value in the specified dimension.
	V(int) float64
}

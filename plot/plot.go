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

package plot

// XYs implements the gonum.org/v1/plot/plotter.XYer interface.
type XYs []XY

// XY is an x and y value.
type XY struct{ X, Y float64 }

// Len returns the number of X,Y pairs.
func (xys XYs) Len() int {
	return len(xys)
}

// XY return the x and y values at index i, where i < Len()
func (xys XYs) XY(i int) (float64, float64) {
	return xys[i].X, xys[i].Y
}

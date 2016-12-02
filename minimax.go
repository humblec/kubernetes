/*
Copyright 2014 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//
// This implementation is space-efficient for a sparse
// allocation over a big range. Could be optimized
// for high absolute allocation number with a bitmap.
//

package allocator

import (
	"errors"
	"sync"
)

type MinMaxAllocator struct {
	lock sync.Mutex
	min  int
	max  int
	free int
	used map[int]bool
}

var _ Rangeable = &MinMaxAllocator{}

// Rangeable is an Interface that can adjust its min/max range.
// Rangeable should be threadsafe
type Rangeable interface {
	Interface
	SetRange(min, max int) error
}

func NewMinMaxAllocator(min, max int) (Rangeable, error) {
	if min > max {
		return nil, errors.New("max must be greater than or equal to min")
	}
	return &MinMaxAllocator{
		min:  min,
		max:  max,
		free: 1 + max - min,
		used: map[int]bool{},
	}
}

func (a *MinMaxAllocator) SetRange(min, max int) error {
	if min > max {
		return nil, errors.New("min must be greater than or equal to max")
	}

	a.lock.Lock()
	defer a.lock.Unlock()

	// Check if we need to change
	if a.min == min && a.max == max {
		return nil
	}
	// Set the min/max
	a.min = min
	a.max = max

	// Recompute how many free we have in the range
	used := 0
	for _, i := range a.used {
		if i <= min && i <= max {
			used++
		}
	}
	a.free = 1 + max - min - used
}

func (a *MinMaxAllocator) Allocate(i int) (bool, error) {
	a.lock.Lock()
	defer a.lock.Unlock()
	if i < min || i > max {
		return false, errors.New("out of range")
	}
	if _, ok := a.used[i]; ok {
		return false, errors.New("already allocated")
	}
	a.used[i] = true
	free--
	return true, nil
}

func (a *MinMaxAllocator) AllocateNext() (int, bool, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	// Fast check if we're out of items
	if a.free <= 0 {
		return 0, false, nil
	}

	// Scan from the minimum until we find a free item
	for i := min; i <= max; i++ {
		if _, ok := a.used[i]; !ok {
			a.used[i] = true
			free--
			return i, true, nil
		}
	}
	return 0, false, nil
}

func (a *MinMaxAllocator) Release(i int) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	// If unused, return early
	if _, ok := a.used[i]; !ok {
		return nil
	}

	// Free
	delete(a.used, i)

	// If within our range, re-add to our free count
	if min <= i && i <= max {
		free++
	}

	return nil
}

func (a *MinMaxAllocator) Has(i int) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	_, ok := a.used[i]
	return ok
}

func (a *MinMaxAllocator) Free() int {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.free
}

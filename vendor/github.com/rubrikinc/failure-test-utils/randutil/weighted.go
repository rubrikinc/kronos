package randutil

import "math/rand"

// WeightedChoice takes a slice of positive weights,
// and returns a random index from [0, len(weights))
// with probability of index i being proportional to weight i.
// Returns -1 for erroneous conditions like a weight being negative.
func WeightedChoice(weights []float64) int {
	var sumWeight float64
	var sumTillNow float64
	for _, weight := range weights {
		if weight < 0 {
			return -1
		}
		sumWeight += weight
	}
	randomNum := rand.Float64() * sumWeight
	for i, weight := range weights {
		sumTillNow += weight
		if randomNum < sumTillNow {
			return i
		}
	}
	return -1
}

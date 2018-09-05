package cli

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateMapping(t *testing.T) {
	testCases := []struct {
		oldToNewAddr map[string]string
		expected     bool
		errRegexp    *regexp.Regexp
	}{
		{
			oldToNewAddr: map[string]string{
				"1.2.3.4": "4.3.2.1",
				"2.3.4.1": "1.4.3.2",
				"3.4.1.2": "2.1.4.3",
			},
			expected: false,
		},
		{
			oldToNewAddr: map[string]string{
				"1.2.3.4:1234": "4.3.2.1:4321",
				"2.3.4.1:2341": "1.4.3.2:1432",
				"3.4.1.2:3412": "2.1.4.3:2143",
			},
			expected: true,
		},
		{
			oldToNewAddr: map[string]string{
				"1.2.3.4": "",
				"2.3.4.1": "1.4.3.2",
				"3.4.1.2": "2.1.4.3",
			},
			errRegexp: regexp.MustCompile("key or value .* is empty"),
		},
		{
			oldToNewAddr: map[string]string{
				"1.2.3.4":      "4.3.2.1",
				"2.3.4.1":      "1.4.3.2",
				"3.4.1.2:3412": "2.1.4.3:2143",
			},
			errRegexp: regexp.MustCompile("some other address"),
		},
		{
			oldToNewAddr: map[string]string{
				"1.2.3.4:1234": "4.3.2.1:4321",
				"2.3.4.1":      "1.4.3.2",
				"3.4.1.2:3412": "2.1.4.3:2143",
			},
			errRegexp: regexp.MustCompile("some other address"),
		},
	}

	for _, tc := range testCases {
		actual, err := validateMapping(tc.oldToNewAddr)
		if err != nil {
			if tc.errRegexp != nil {
				assert.Regexp(t, tc.errRegexp, err.Error())
			}
		}
		assert.Equal(t, tc.expected, actual)
	}
}

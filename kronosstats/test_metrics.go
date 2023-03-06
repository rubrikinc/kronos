package kronosstats

type mockGauge struct {
	value int64
}

func (m *mockGauge) Value() int64 {
	return m.value
}

func (m *mockGauge) Update(v int64) {
	m.value = v
}

type mockCounter struct {
	count int64
}

func (m *mockCounter) Inc(i int64) {
	m.count += i
}

type mockHistogram struct {
}

func (*mockHistogram) RecordValue(_ int64) {

}

func NewTestMetrics() *KronosMetrics {
	return &KronosMetrics{
		Delta:                 &mockGauge{},
		UptimeDelta:           &mockGauge{},
		IsOracle:              &mockGauge{},
		OverthrowAttemptCount: &mockCounter{},
		RTT:                   &mockHistogram{},
		SyncSuccessCount:      &mockCounter{},
		SyncFailureCount:      &mockCounter{},
		TimeCap:               &mockGauge{},
		UptimeCap:             &mockGauge{},
	}
}

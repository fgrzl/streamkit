package cache

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkCache_Set(b *testing.B) {
	c := NewExpiringCache(5*time.Minute, time.Hour)
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(strconv.Itoa(i), i)
	}
}

func BenchmarkCache_Get_Hit(b *testing.B) {
	c := NewExpiringCache(5*time.Minute, time.Hour)
	c.Set("k", 123)
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := c.Get("k"); !ok {
			b.Fatalf("miss")
		}
	}
}

func BenchmarkCache_Get_Miss(b *testing.B) {
	c := NewExpiringCache(5*time.Minute, time.Hour)
	defer c.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get("nope")
	}
}

func BenchmarkCache_Mixed(b *testing.B) {
	c := NewExpiringCache(5*time.Minute, time.Hour)
	defer c.Close()
	// prefill
	for i := 0; i < 1000; i++ {
		c.Set(strconv.Itoa(i), i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := strconv.Itoa(i % 1000)
		if i%3 == 0 {
			c.Set(k, i)
		} else {
			_, _ = c.Get(k)
		}
	}
}

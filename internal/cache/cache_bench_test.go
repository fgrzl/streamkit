package cache

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkCache_Set(b *testing.B) {
	c := NewExpiringCache(5*time.Minute, time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(strconv.Itoa(i), i)
	}
	c.Close()
}

func BenchmarkCache_Get_Hit(b *testing.B) {
	c := NewExpiringCache(5*time.Minute, time.Hour)
	c.Set("k", 123)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := c.Get("k"); !ok {
			b.Fatal("miss")
		}
	}
	c.Close()
}

func BenchmarkCache_Get_Miss(b *testing.B) {
	c := NewExpiringCache(5*time.Minute, time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Get("nope")
	}
	c.Close()
}

func BenchmarkCache_Mixed(b *testing.B) {
	c := NewExpiringCache(5*time.Minute, time.Hour)
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
	c.Close()
}

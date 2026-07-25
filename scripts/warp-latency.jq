.operations
| if length != 1 then error("expected one Warp operation") else .[0] end
| if (.skipped // false) then error("Warp skipped request statistics") else . end
| . as $operation
| $operation.single_sized_requests
| if . == null or (.skipped // false) then
    error("expected single-sized request statistics")
  else .
  end
| . as $requests
| {
    throughput: {
      bytes_per_second:
        ($operation.throughput.bytes * 1000 / $operation.throughput.measure_duration_millis),
      operations_per_second:
        ($operation.throughput.ops * 1000 / $operation.throughput.measure_duration_millis),
      objects_per_second:
        ($operation.throughput.objects * 1000 / $operation.throughput.measure_duration_millis)
    },
    request: {
      average: $requests.dur_avg_millis,
      p50: $requests.dur_median_millis,
      p90: $requests.dur_90_millis,
      p99: $requests.dur_99_millis,
      min: $requests.fastest_millis,
      max: $requests.slowest_millis
    }
  }
| if $requests.first_byte == null then .
  else {
    throughput: .throughput,
    request: .request,
    ttfb: {
      average: $requests.first_byte.average_millis,
      p50: $requests.first_byte.median_millis,
      p90: $requests.first_byte.p90_millis,
      p99: $requests.first_byte.p99_millis,
      min: $requests.first_byte.fastest_millis,
      max: $requests.first_byte.slowest_millis
    }
  }
  end

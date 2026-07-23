.operations
| if length != 1 then error("expected one Warp operation") else .[0] end
| if (.skipped // false) then error("Warp skipped request statistics") else . end
| .single_sized_requests
| if . == null or (.skipped // false) then
    error("expected single-sized request statistics")
  else .
  end
| . as $requests
| {
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

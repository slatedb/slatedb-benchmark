.by_op_type
| to_entries
| if length != 1 then error("expected one Warp operation") else .[0].value end
| [
    .requests_by_client[]
    | .[]
    | .single_sized_requests
    | select(. != null and ((.skipped // false) | not))
  ]
| if length == 0 then error("expected single-sized request statistics") else . end
| . as $segments
| ($segments | map(.merged_entries // 1) | add) as $merged
| if $merged <= 0 then error("invalid merged entry count") else . end
| ($segments | map(select(.first_byte != null))) as $ttfb_segments
| {
    request: {
      average: (($segments | map(.dur_avg_millis) | add) / $merged),
      p50: (($segments | map(.dur_median_millis) | add) / $merged),
      p90: (($segments | map(.dur_90_millis) | add) / $merged),
      p99: (($segments | map(.dur_99_millis) | add) / $merged),
      min: ($segments | map(.fastest_millis) | min),
      max: ($segments | map(.slowest_millis) | max)
    }
  }
| if ($ttfb_segments | length) == 0 then .
  else {
    request: .request,
    ttfb: (
      ($ttfb_segments | map(.merged_entries // 1) | add) as $ttfb_merged
      | {
        average: (($ttfb_segments | map(.first_byte.average_millis) | add) / $ttfb_merged),
        p50: (($ttfb_segments | map(.first_byte.median_millis) | add) / $ttfb_merged),
        p90: (($ttfb_segments | map(.first_byte.p90_millis) | add) / $ttfb_merged),
        p99: (($ttfb_segments | map(.first_byte.p99_millis) | add) / $ttfb_merged),
        min: ($ttfb_segments | map(.first_byte.fastest_millis) | min),
        max: ($ttfb_segments | map(.first_byte.slowest_millis) | max)
      }
    )
  }
  end

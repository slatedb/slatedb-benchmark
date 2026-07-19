export const S3_STANDARD_REQUEST_PRICING = {
  name: 'Amazon S3 Standard',
  region: 'US East (N. Virginia)',
  source: 'https://aws.amazon.com/s3/pricing/',
  monthDays: 30,
  requestsPer1000Usd: {
    GET: 0.0004,
    PUT: 0.005,
    HEAD: 0.0004,
    DELETE: 0,
    POST: 0.005,
    OTHER: 0.0004,
  },
} as const;

export function s3RequestPricePer1000(method: string): number {
  const prices = S3_STANDARD_REQUEST_PRICING.requestsPer1000Usd;
  return method in prices ? prices[method as keyof typeof prices] : prices.OTHER;
}

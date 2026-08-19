# GitHub benchmark setup

The benchmark workflows need three external pieces:

1. WarpBuild runners for the benchmark jobs.
2. An object-store environment named `benchmark-s3` or `benchmark-tigris`.
3. GitHub Pages for the published website.

The examples below target `slatedb/slatedb-benchmark`. Replace the repository,
AWS account, region, bucket, and role names when setting up a fork.

## Prerequisites

You need:

- Admin access to the GitHub repository and, for WarpBuild, its organization.
- Permission to create an S3 bucket, IAM identity provider, policy, and role.
- A [WarpBuild account](https://www.warpbuild.com/docs/ci/quick-start).
- `gh` and the AWS CLI if you want to use the command-line examples.

Authenticate the GitHub CLI and select the repository:

```console
$ gh auth login
$ gh repo set-default slatedb/slatedb-benchmark
```

## Configure WarpBuild

Sign in to WarpBuild, install the WarpBuild GitHub app, and grant it access to
this repository. The workflows use these runner labels:

| Label | Jobs |
| --- | --- |
| `warp-ubuntu-latest-arm64-8x` | Golden data and transfer-capacity |
| `warp-ubuntu-latest-arm64-16x` | Build, bundle, publish, and cleanup |
| `warp-custom-ubuntu-latest-arm64-32x-300gb` | Workload matrix |

Confirm that all three labels appear in the WarpBuild dashboard. If this is a
public repository, WarpBuild also requires the setup described in
[Public GitHub Repos](https://www.warpbuild.com/docs/ci/public-repos).

Follow WarpBuild's
[non-default runner group instructions](https://www.warpbuild.com/docs/ci/common-issues#default-runner-group)
to configure the runners in the GitHub organization's **Performance** runner
group:

1. Open **Settings > Actions > Runner groups > Performance**.
2. Set **Repository access** to **Selected repositories** and select
   `slatedb/slatedb` and `slatedb/slatedb-benchmark`.
3. Enable **Allow public repositories**.
4. Set **Workflow access** to **Selected workflows**.
5. Add these exact workflow references:

   ```text
   slatedb/slatedb/.github/workflows/nightly.yaml@refs/heads/main
   slatedb/slatedb-benchmark/.github/workflows/benchmark.yml@refs/heads/main
   slatedb/slatedb-benchmark/.github/workflows/golden.yml@refs/heads/main
   slatedb/slatedb-benchmark/.github/workflows/transfer-capacity.yml@refs/heads/main
   ```

GitHub matches the complete repository, workflow path, and Git ref. A shortened
name, the wrong `.yml` or `.yaml` extension, or a missing
`@refs/heads/main` will not grant the workflow access to the runners.

Jobs that wait indefinitely at “Waiting for a runner to pick up this job”
usually indicate that WarpBuild cannot access the repository or that the
requested runner label is unavailable.

## Configure Amazon S3

### Create the bucket

Create a private S3 bucket in the region where the benchmark should run. Keep
Block Public Access enabled. The website reads published results from Git, so
the bucket does not need public access or CORS.

This guide uses:

```text
Region:  us-east-1
Bucket:  YOUR_BUCKET
Prefix:  benchmark
```

The workflows retain golden datasets and delete successful workload sessions.
An S3 lifecycle rule that aborts incomplete multipart uploads is useful, but do
not expire the entire `benchmark/` prefix unless golden datasets are disposable.

### Add GitHub as an IAM identity provider

In AWS IAM, open **Identity providers**, add an OpenID Connect provider, and
use:

```text
Provider URL: https://token.actions.githubusercontent.com
Audience:     sts.amazonaws.com
```

Reuse the provider if it already exists in the account. GitHub's
[AWS OIDC guide](https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-aws)
describes the same provider and audience.

### Create the S3 access policy

Create an IAM policy named `slatedb-benchmark-s3`. Replace `YOUR_BUCKET` in
this policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BucketMetadata",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucketMultipartUploads"
      ],
      "Resource": "arn:aws:s3:::YOUR_BUCKET"
    },
    {
      "Sid": "ListBenchmarkPrefix",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::YOUR_BUCKET",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "benchmark",
            "benchmark/*"
          ]
        }
      }
    },
    {
      "Sid": "BenchmarkObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": "arn:aws:s3:::YOUR_BUCKET/benchmark/*"
    }
  ]
}
```

### Create the GitHub Actions role

Create an IAM role named `slatedb-benchmark-ci`, attach the
`slatedb-benchmark-s3` policy, and use this trust policy. Replace
`YOUR_ACCOUNT_ID` if needed:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::YOUR_ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
          "token.actions.githubusercontent.com:sub": "repo:slatedb/slatedb-benchmark:environment:benchmark-s3"
        }
      }
    }
  ]
}
```

The `sub` condition is important. It limits the role to jobs in this repository
that use the `benchmark-s3` environment. A fork needs its own owner and
repository in that value. If the repository uses GitHub's immutable OIDC
subjects, use the immutable owner and repository IDs shown by GitHub instead.

AWS and GitHub both recommend constraining the OIDC subject rather than trusting
every repository that can request a GitHub token. See AWS's
[GitHub OIDC role guidance](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-idp_oidc.html).

Copy the role ARN after creating it:

```text
arn:aws:iam::YOUR_ACCOUNT_ID:role/slatedb-benchmark-ci
```

## Create the GitHub environments

The workflows resolve object-store configuration from GitHub environments.
Open **Repository settings > Environments** and create `benchmark-s3`.

Add these environment variables:

| Variable | Example |
| --- | --- |
| `AWS_REGION` | `us-east-1` |
| `AWS_ROLE_ARN` | `arn:aws:iam::YOUR_ACCOUNT_ID:role/slatedb-benchmark-ci` |
| `SLATEDB_BENCH_BUCKET` | `YOUR_BUCKET` |

Do not add AWS access keys. The workflow's `id-token: write` permission and
`aws-actions/configure-aws-credentials` exchange the GitHub OIDC token for
short-lived AWS credentials.

Restrict the environment to the `main` branch if benchmarks should only run
from reviewed workflow code. Avoid required reviewers unless manual approval
for every environment job is intentional. GitHub applies environment rules
before assigning a runner or exposing its variables and secrets. See
[Managing environments](https://docs.github.com/en/actions/how-tos/deploy/configure-and-manage-deployments/manage-environments).

The same environment can be created with `gh`:

```console
$ gh api --method PUT repos/slatedb/slatedb-benchmark/environments/benchmark-s3
$ gh variable set AWS_REGION \
    --env benchmark-s3 --body us-east-1
$ gh variable set AWS_ROLE_ARN \
    --env benchmark-s3 \
    --body arn:aws:iam::YOUR_ACCOUNT_ID:role/slatedb-benchmark-ci
$ gh variable set SLATEDB_BENCH_BUCKET \
    --env benchmark-s3 --body YOUR_BUCKET
```

### Optional Tigris environment

To run the same workflows against Tigris, create `benchmark-tigris`.

Add these variables:

| Variable | Value |
| --- | --- |
| `AWS_ENDPOINT_URL_S3` | `https://t3.storage.dev` |
| `AWS_REGION` | `auto` |
| `SLATEDB_BENCH_BUCKET` | Tigris bucket name |

Add these environment secrets:

```text
TIGRIS_ACCESS_KEY_ID
TIGRIS_SECRET_ACCESS_KEY
```

The CLI prompts for each secret without printing it:

```console
$ gh api --method PUT repos/slatedb/slatedb-benchmark/environments/benchmark-tigris
$ gh variable set AWS_ENDPOINT_URL_S3 \
    --env benchmark-tigris --body https://t3.storage.dev
$ gh variable set AWS_REGION \
    --env benchmark-tigris --body auto
$ gh variable set SLATEDB_BENCH_BUCKET \
    --env benchmark-tigris --body YOUR_TIGRIS_BUCKET
$ gh secret set TIGRIS_ACCESS_KEY_ID --env benchmark-tigris
$ gh secret set TIGRIS_SECRET_ACCESS_KEY --env benchmark-tigris
```

## Enable GitHub Actions publication

Open **Repository settings > Actions > General** and make sure Actions are
enabled. If the organization restricts third-party actions, allow the actions
referenced in `.github/workflows/`, including `aws-actions`, `dtolnay`, and
`Swatinem`.

The workflows request narrow `GITHUB_TOKEN` permissions in YAML. Organization
or repository policy must allow:

- `contents: write` so the publish job can commit results to `main`.
- `actions: write` so the publish job can dispatch `pages.yml`.
- `pages: write` and `id-token: write` for the Pages deployment.

The publisher pushes result commits directly to `main`. Branch protection or a
repository ruleset must allow `github-actions[bot]` to make those commits. The
current workflow does not open a pull request or use a separate personal access
token.

## Configure GitHub Pages

Open **Repository settings > Pages** and select **GitHub Actions** as the
publishing source. The `pages.yml` workflow builds `website/`, uploads
`website/dist`, and deploys through the `github-pages` environment. GitHub may
create that environment on the first deployment.

The checked-in Astro configuration uses `https://benchmark.slatedb.io`. For a
fork, update `website/astro.config.mjs` to the new site URL. To keep the current
domain, configure `benchmark.slatedb.io` under Pages and point its DNS record at
the GitHub Pages host. GitHub documents the workflow source in
[Configuring a publishing source](https://docs.github.com/en/pages/getting-started-with-github-pages/configuring-a-publishing-source-for-your-github-pages-site).

## Run a smoke benchmark

Run golden preparation first. The benchmark workflow requires a golden dataset
with the same object store, ID, and scale.

```console
$ gh workflow run golden.yml \
    -f slatedb_ref=main \
    -f golden_id=golden-smoke \
    -f object_store=s3 \
    -f scale=0.00001
```

Wait for both golden jobs to finish, then run the benchmark:

```console
$ gh workflow run benchmark.yml \
    -f slatedb_ref=main \
    -f golden_id=golden-smoke \
    -f object_store=s3 \
    -f scale=0.00001
```

Every successful benchmark run publishes its results and dispatches the Pages
workflow, including scaled runs. Use a new golden ID after changing its scale,
SlateDB source, patches, or preparation settings.

The transfer-capacity probe is independent:

```console
$ gh workflow run transfer-capacity.yml \
    -f object_store=s3 \
    -f scale=0.01
```

Successful probes publish a summarized result and rebuild the website. Raw
Warp request data remains available only in the workflow artifact.

## Troubleshooting

### `Not authorized to perform sts:AssumeRoleWithWebIdentity`

Check `AWS_ROLE_ARN`, the OIDC audience, and the role's `sub` condition. The
subject must name the `benchmark-s3` environment exactly.

### `Waiting for a runner to pick up this job`

Confirm that the **Performance** runner group grants access to the repository
and the workflow's exact path and ref. Also check that **Allow public
repositories** is enabled and that the requested 8x, 16x, or 32x label exists.

### `golden.json` is missing or rejected

Run `golden.yml` first. Use the same environment, bucket, golden ID, and scale
in both workflows.

### The publish job cannot push

Check that the workflow token may write repository contents and that branch
rules allow `github-actions[bot]` to push to `main`.

### The website does not deploy

Set Pages to use GitHub Actions, inspect the `Deploy benchmark website`
workflow, and check any protection rules on the `github-pages` environment.

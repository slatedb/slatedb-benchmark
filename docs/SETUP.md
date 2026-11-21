# GitHub benchmark setup

The benchmark workflows need three external pieces:

1. Ephemeral AWS EC2 runners in `us-east-1` via `machulav/ec2-github-runner`.
2. An Amazon S3 environment named `benchmark-s3`.
3. GitHub Pages for the published website.

The examples below target `slatedb/slatedb-benchmark`. Replace the repository,
AWS account, region, bucket, and role names when setting up a fork.

## Prerequisites

You need:

- Admin access to the GitHub repository.
- Permission to create an S3 bucket, IAM identity provider, policies, roles,
  VPC endpoint, and security group in AWS.
- `gh` and the AWS CLI if you want to use the command-line examples.

Authenticate the GitHub CLI and select the repository:

```console
$ gh auth login
$ gh repo set-default slatedb/slatedb-benchmark
```

## Configure ephemeral EC2 runners

Build, bundle, publish, and cleanup jobs run on GitHub-hosted `ubuntu-24.04-arm`
runners. Preparation, workload, and transfer-capacity jobs launch single-use JIT
EC2 Graviton runners in `us-east-1` with `machulav/ec2-github-runner`:

| Instance type | Jobs |
| --- | --- |
| `c7g.2xlarge` | Golden data |
| `c7gd.2xlarge` | `*-mirror` workloads (local NVMe mounted at `/mnt/nvme`) |
| `c7g.8xlarge` | Standard workload matrix and transfer-capacity |

Set these repository-level GitHub Actions variables and secrets:

- `AWS_EC2_ROLE_ARN`: OIDC IAM role (`slatedb-benchmark-ec2-runner`) with
  `ec2:RunInstances`, `ec2:TerminateInstances`, `ec2:DescribeInstances`,
  `ec2:DescribeInstanceStatus`, `ec2:CreateTags`, and `ec2:GetConsoleOutput`
  scoped to `Project=slatedb-benchmark` instances.
- `AWS_EC2_REGION`: `us-east-1`.
- `EC2_AVAILABILITY_ZONES_CONFIG`: JSON array of `{imageId, subnetId, securityGroupId}`
  entries across `c7gd`-capable subnets in `us-east-1` using an Ubuntu 24.04 ARM64
  AMI and an outbound-only security group (paired with a free S3 Gateway VPC
  Endpoint on the VPC route table).
- `EC2_RUNNER_GITHUB_TOKEN` (secret): fine-grained PAT scoped to this repository
  with `Administration: Read and write` for JIT runner registration.

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

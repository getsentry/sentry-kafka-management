## 0.0.9

- No documented changes.

## 0.0.8

### New Features ✨

- feat: add script to delete configs from record dir by @bmckerry in [#31](https://github.com/getsentry/sentry-kafka-management/pull/31)
- feat: add script to delete configs from record dir by @bmckerry in [#31](https://github.com/getsentry/sentry-kafka-management/pull/31)
- feat(release): Add --version option to CLI by @lvthanh03 in [#30](https://github.com/getsentry/sentry-kafka-management/pull/30)
- feat(brokers): add ability to record config changes by @bmckerry in [#27](https://github.com/getsentry/sentry-kafka-management/pull/27)
- feat(brokers): add remove_dynamic_configs script by @bmckerry in [#26](https://github.com/getsentry/sentry-kafka-management/pull/26)
- feat(brokers): add remove_dynamic_configs script by @bmckerry in [#26](https://github.com/getsentry/sentry-kafka-management/pull/26)

### Build / dependencies / internal 🔧

- ref(brokers): split apply_configs logic into separate function by @bmckerry in [#25](https://github.com/getsentry/sentry-kafka-management/pull/25)

### Other

- Fix release by @lvthanh03 in [#32](https://github.com/getsentry/sentry-kafka-management/pull/32)
- feat(broker-configs): Split update configs to 1 request per change by @lvthanh03 in [#28](https://github.com/getsentry/sentry-kafka-management/pull/28)
- meta: add codeowners to the repo by @bmckerry in [#24](https://github.com/getsentry/sentry-kafka-management/pull/24)

## 0.0.7

- feat(config): Add apply-config function by @lvthanh03 in [#22](https://github.com/getsentry/sentry-kafka-management/pull/22)
- chore: Push images to SR and MR registries by @lvthanh03 in [#21](https://github.com/getsentry/sentry-kafka-management/pull/21)

## 0.0.6

### Various fixes & improvements

- Use KAFKA_TIMEOUT from config (#19) by @lvthanh03
- Add mypy type ignore (#19) by @lvthanh03
- Fix mock values in tests (#19) by @lvthanh03
- feat: add script for describe cluster (#19) by @lvthanh03

## 0.0.5

### Various fixes & improvements

- feat: add action for describing broker configs (#20) by @bmckerry
- feat: switch from argparse to click (#18) by @bmckerry
- clarify build action names (#17) by @bmckerry

## 0.0.4

### Various fixes & improvements

- feat: use devinfra standard build/push workflow (#16) by @bmckerry
- unified config file (#15) by @bmckerry
- add tests for cli/scripts (#6) by @enochtangg
- fix connection & tests (#6) by @enochtangg
- WIP: functional scripts for local docker (#6) by @enochtangg
- Make scripts non-executables, add epilog (#6) by @enochtangg
- create underpriv user, install with uv, and add .dockerignore (#6) by @enochtangg
- Create docker file and organize CLI (#6) by @enochtangg

## 0.0.3

### Various fixes & improvements

- Use python 3.11 (#13) by @fpacifici
- track config source (#11) by @bmckerry
- address comments (#11) by @bmckerry
- feat: add script for getting broker configs (#11) by @bmckerry

## 0.0.1

### Various fixes & improvements

- Add build script (#12) by @fpacifici
- Remove changelog from pre-commit hook (#10) by @fpacifici
- Fix craft descriptor (#9) by @fpacifici
- Add changelog (#8) by @fpacifici


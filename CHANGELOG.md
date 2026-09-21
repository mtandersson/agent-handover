## [0.12.0](https://github.com/mtandersson/agent-handover/compare/v0.11.0...v0.12.0) (2026-09-21)

### Features

* add privacy-safe lifecycle logs ([5259234](https://github.com/mtandersson/agent-handover/commit/5259234f2a96f894f84c6a680bbfa9c0b65850f5))

### Bug Fixes

* handle Notion journal date precision ([31dadbf](https://github.com/mtandersson/agent-handover/commit/31dadbf1fc6ced4de74d75900bbed98700bc5e5f))

### Tests

* **state:** isolate temporary state directories ([8aec189](https://github.com/mtandersson/agent-handover/commit/8aec18991b1d68242d6aaafc016775fa370daf2d))

## [0.11.0](https://github.com/mtandersson/agent-handover/compare/v0.10.0...v0.11.0) (2026-09-13)

### Features

* **cli:** add actionable command help ([#66](https://github.com/mtandersson/agent-handover/issues/66)) ([6b04809](https://github.com/mtandersson/agent-handover/commit/6b048094095047a2454e7e25ac117a4763d34676))
* **cloudflared:** prepare private tunnel ingress ([2962b4d](https://github.com/mtandersson/agent-handover/commit/2962b4d33ed2723ddb5acb907c6e4c853f56951f))
* **cloudflared:** supervise credential tunnel enrollment ([d5d6045](https://github.com/mtandersson/agent-handover/commit/d5d6045c4fe72d59551ed3b92d4e620f6444bc09))
* **cloudflared:** supervise remote token enrollment ([7036440](https://github.com/mtandersson/agent-handover/commit/7036440e538cd6a9643fce39fd73e2e5bae064df))
* **cloudflared:** verify remote tunnel ingress ([00d284f](https://github.com/mtandersson/agent-handover/commit/00d284fb79c4137b662b83328e0632dff384ccb8))
* **enrollment:** receive verification on secret path ([#73](https://github.com/mtandersson/agent-handover/issues/73)) ([de73341](https://github.com/mtandersson/agent-handover/commit/de733413fb7f08c95d0619335c6db1ad22a54e7d)), closes [#70](https://github.com/mtandersson/agent-handover/issues/70)
* serve webhooks on enrolled secret path ([ca2daea](https://github.com/mtandersson/agent-handover/commit/ca2daeadb0b4300c004c720d6ca73633a275c82d))
* **serving:** supervise managed Cloudflare tunnels ([f5e84b4](https://github.com/mtandersson/agent-handover/commit/f5e84b4d85961e68a470a590d7e77695573723ea))

### Bug Fixes

* **cli:** show command-specific help ([5bdd754](https://github.com/mtandersson/agent-handover/commit/5bdd754f0d43ad1369e5d485df085250abd515f8))

### Documentation

* **cloudflare:** guide managed tunnel operation ([c4436d5](https://github.com/mtandersson/agent-handover/commit/c4436d56ceb3ec75ed3f8c25bf269676315e8c04))
* guide secret webhook enrollment ([#76](https://github.com/mtandersson/agent-handover/issues/76)) ([bf0b793](https://github.com/mtandersson/agent-handover/commit/bf0b793d8fd63b5bd8849c639e463e37ca89b2fa))

### Chores

* **deps:** update rust crate toml to v1.1.6 ([#74](https://github.com/mtandersson/agent-handover/issues/74)) ([59ff8df](https://github.com/mtandersson/agent-handover/commit/59ff8dfe2289701ed03d87dac72827b83f2f6726))

## [0.10.0](https://github.com/mtandersson/agent-handover/compare/v0.9.0...v0.10.0) (2026-09-09)

### Features

* finalize durable Notion attempts ([8f33645](https://github.com/mtandersson/agent-handover/commit/8f336459615fc32e4ba017bcad872f87c8158abe))
* **orchestration:** execute discovered tasks ([67ab1a9](https://github.com/mtandersson/agent-handover/commit/67ab1a911e32efb8f55ff9eb449e757c5ac8a2df))
* **state:** enforce durable launch authority ([7b4a70a](https://github.com/mtandersson/agent-handover/commit/7b4a70a8f3f94ef89d212c32ffea64f5f274149f))

### Bug Fixes

* **recovery:** finalize interrupted launches as unknown ([fde70b0](https://github.com/mtandersson/agent-handover/commit/fde70b0aad3a0ded6dc6f04b5aafaf95e3c101b0))

### Tests

* specify manual error retry lifecycle ([#64](https://github.com/mtandersson/agent-handover/issues/64)) ([0de67ef](https://github.com/mtandersson/agent-handover/commit/0de67efbfcad05bebf8d8af89168a6e432cb354d))

### Chores

* **deps:** update rust crate reqwest to v0.13.5 ([#65](https://github.com/mtandersson/agent-handover/issues/65)) ([d80fadd](https://github.com/mtandersson/agent-handover/commit/d80fadd504b225a7aba2d449ac81352db27d80df))

## [0.9.0](https://github.com/mtandersson/agent-handover/compare/v0.8.0...v0.9.0) (2026-09-07)

### Features

* **executor:** add Codex CLI adapter ([0484132](https://github.com/mtandersson/agent-handover/commit/0484132c2e2ed7966f9b6a4b9fae2bf590c1762e))

### Chores

* **deps:** update rust crate toml to v1.1.5 ([#56](https://github.com/mtandersson/agent-handover/issues/56)) ([f2a9def](https://github.com/mtandersson/agent-handover/commit/f2a9def97d8aa827e53b8b417549c8878452ed19))

## [0.8.0](https://github.com/mtandersson/agent-handover/compare/v0.7.0...v0.8.0) (2026-09-01)

### Features

* coordinate discovered task revisions ([010fd8a](https://github.com/mtandersson/agent-handover/commit/010fd8ac296ba6175045adf74310602099969e61))
* prepare visible Notion attempts ([6738620](https://github.com/mtandersson/agent-handover/commit/67386208e5c4cbb89b9435dab19f0c254e283b9b))
* reconcile pending tasks during serve ([da84b68](https://github.com/mtandersson/agent-handover/commit/da84b685b6f31a7749196deb3d1900fedff39745))
* reconcile Pending tasks with run-once ([63b3834](https://github.com/mtandersson/agent-handover/commit/63b383439a04a4a12670561b807648f2c1be990b))
* **state:** persist exclusive prepared attempts ([63be5a3](https://github.com/mtandersson/agent-handover/commit/63be5a384cb931fc4187831260661cf7d9579e01))

### Bug Fixes

* **build:** refresh release tooling dependency hash ([f206d53](https://github.com/mtandersson/agent-handover/commit/f206d532c6ec12ac29950b4a6e33c555c23dee3e))
* **deps:** align sha2 with hmac 0.13 ([1f95e55](https://github.com/mtandersson/agent-handover/commit/1f95e55087235d5bdf1c9701d3350a04545a5005))
* **deps:** exclude Nix-pinned npm tooling from Renovate ([d25cfe1](https://github.com/mtandersson/agent-handover/commit/d25cfe1b52ca2065c8a3104b3a50a83fd3b3a48a))
* **deps:** migrate reqwest rustls configuration ([1f2fa0c](https://github.com/mtandersson/agent-handover/commit/1f2fa0c7ccec7484d81fa7d0f496032b3c0998bf))
* **deps:** update dependency conventional-changelog-conventionalcommits to v9.3.1 ([53dba19](https://github.com/mtandersson/agent-handover/commit/53dba19e39b7c5df3223e4eb885d18908f467b64))
* **deps:** update rust crate hmac to 0.13 ([e27a704](https://github.com/mtandersson/agent-handover/commit/e27a70493fbd99c5fcfd4b6780b8fda2b2a34b56))
* **deps:** update rust crate reqwest to 0.13 ([d851fe3](https://github.com/mtandersson/agent-handover/commit/d851fe3c42faac96886395f3a7c92faceaa1c913))
* **deps:** update rust crate toml to v1 ([0b5c61a](https://github.com/mtandersson/agent-handover/commit/0b5c61ad31f067b456be3feb9eb1a5d469a0c7dd))

### Performance Improvements

* **ci:** cache reusable Nix build artifacts ([3939ea8](https://github.com/mtandersson/agent-handover/commit/3939ea81e2da94eea9cb9c6791404d6e7bb4922d))

### Tests

* **ci:** allow process startup before timeout assertion ([6715d72](https://github.com/mtandersson/agent-handover/commit/6715d72c9c5c315e7f1fa0e0610975b29fef649c))

### Continuous Integration

* automerge Renovate updates after checks pass ([ebe6139](https://github.com/mtandersson/agent-handover/commit/ebe61399a437fac687fca62fb3aa8379835a3c55))
* schedule weekly releases ([1484974](https://github.com/mtandersson/agent-handover/commit/14849749a0d1f544c459245cf37d3b5f2897b39f))

## [0.7.0](https://github.com/mtandersson/agent-handover/compare/v0.6.2...v0.7.0) (2026-08-31)

### Features

* **config:** resolve Notion token from ntn ([65138fc](https://github.com/mtandersson/agent-handover/commit/65138fc5a2faa2410e2ed600ff0a325363609095))

## [0.6.2](https://github.com/mtandersson/agent-handover/compare/v0.6.1...v0.6.2) (2026-08-31)

### Bug Fixes

* **deps:** update rust crate time to v0.3.55 ([d677898](https://github.com/mtandersson/agent-handover/commit/d6778986e9e95919e6483d560ff6649c9bd02bdb))

## [0.6.1](https://github.com/mtandersson/agent-handover/compare/v0.6.0...v0.6.1) (2026-08-31)

### Bug Fixes

* **config:** keep executor choice private ([e1d753b](https://github.com/mtandersson/agent-handover/commit/e1d753b5557a37e9514f06803162df127f1d18ed))

### Chores

* **deps:** configure Renovate commits ([38366b6](https://github.com/mtandersson/agent-handover/commit/38366b666f7924c8be198271a0e52ee3fc72f707))

## [0.6.0](https://github.com/mtandersson/agent-handover/compare/v0.5.0...v0.6.0) (2026-08-31)

### Features

* **notion:** render task page instructions ([b83eaa3](https://github.com/mtandersson/agent-handover/commit/b83eaa3604e08fcc99240e03f4db704222c17dfd))

## [0.5.0](https://github.com/mtandersson/agent-handover/compare/v0.4.0...v0.5.0) (2026-08-30)

### Features

* **notion:** filter webhook task signals ([1d0a258](https://github.com/mtandersson/agent-handover/commit/1d0a2584faf0761e023f7387d6670b68ac54c575))

## [0.4.0](https://github.com/mtandersson/agent-handover/compare/v0.3.0...v0.4.0) (2026-08-30)

### Features

* **webhook:** authenticate HTTP intake ([1429f83](https://github.com/mtandersson/agent-handover/commit/1429f837f1c82734f40c54c5b5d8de400b86fd82))

## [0.3.0](https://github.com/mtandersson/agent-handover/compare/v0.2.0...v0.3.0) (2026-08-30)

### Features

* **webhook:** enroll verification tokens ([7459da4](https://github.com/mtandersson/agent-handover/commit/7459da406e876405407458cc65a1f57ed0c311d1))

## [0.2.0](https://github.com/mtandersson/agent-handover/compare/v0.1.0...v0.2.0) (2026-08-30)

### Features

* **config:** add private runner foundation ([3c34fce](https://github.com/mtandersson/agent-handover/commit/3c34fce82e25abc297b3358dc54537cce4cd5bca))

### Documentation

* publish alpha project contract ([37d030d](https://github.com/mtandersson/agent-handover/commit/37d030d8cee6bbb1d9814b5ab5b1813be8fdd8fc))
* **skill:** strengthen issue scope gate ([01170b2](https://github.com/mtandersson/agent-handover/commit/01170b288a711ded980f64a83acaed493e16e341))

### Continuous Integration

* configure identity for title validation ([84ec858](https://github.com/mtandersson/agent-handover/commit/84ec858c8130006832910e25f3bb93fb50f736de))

## [0.1.0](https://github.com/mtandersson/agent-handover/compare/v0.0.0...v0.1.0) (2026-08-30)

### Features

* bootstrap Rust release pipeline ([3bb5131](https://github.com/mtandersson/agent-handover/commit/3bb5131185102ade5af4d63e8b33a9a3f0a09da5))

### Bug Fixes

* **ci:** fetch release tags during checkout ([3c951a4](https://github.com/mtandersson/agent-handover/commit/3c951a41d10cd66d508633c4e932b3d54bec7cee))
* **ci:** validate semantic release tag range ([d12ffd9](https://github.com/mtandersson/agent-handover/commit/d12ffd9ed1c326a88fd64d645137fe7b5a4d038c))

# Changelog

All notable changes to this project are documented in this file. Releases are
generated automatically from Conventional Commits.

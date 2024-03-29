0.4.3 (2022-02-08)
==================
* [#49](https://github.com/trocco-io/embulk-output-snowflake/pull/49) fix oom in large file input

0.4.5 (2022-04-05)
==================
* [#47](https://github.com/trocco-io/embulk-output-snowflake/pull/47) Fix drop stage not exists

0.4.4 (2022-03-30)
==================
* [#43](https://github.com/trocco-io/embulk-output-snowflake/pull/43) fix lint #43
* [#44](https://github.com/trocco-io/embulk-output-snowflake/pull/44) add test in case of another db which has same table exists #44
* [#45](https://github.com/trocco-io/embulk-output-snowflake/pull/45) allow multi query #45

0.4.3 (2022-02-08)
==================
* [#40](https://github.com/trocco-io/embulk-output-snowflake/pull/40) revert #37
* [#41](https://github.com/trocco-io/embulk-output-snowflake/pull/41) set CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX=true

0.4.2 (2022-01-28)
==================
* [#37](https://github.com/trocco-io/embulk-output-snowflake/pull/37) fix duplicate column in the insert mode

0.4.1 (2022-01-28)
==================
* [#35](https://github.com/trocco-io/embulk-output-snowflake/pull/35) bump up snowflake-jdbc version to 3.13.14

0.4.0 (2021-10-08)
======================
* [#21](https://github.com/trocco-io/embulk-output-snowflake/pull/21) Revert [#14](https://github.com/trocco-io/embulk-output-snowflake/pull/14)
* [#27](https://github.com/trocco-io/embulk-output-snowflake/pull/27) Add an example.
* [#28](https://github.com/trocco-io/embulk-output-snowflake/pull/28), [#30](https://github.com/trocco-io/embulk-output-snowflake/pull/30) Add tests
* [#32](https://github.com/trocco-io/embulk-output-snowflake/pull/32) Use embulk-output-jdbc v0.10.2 that fixes the issue [embulk/embulk-output-jdbc#299](https://github.com/embulk/embulk-output-jdbc/issues/299).


0.3.3 (2021-09-16)
==================

* [#25](https://github.com/trocco-io/embulk-output-snowflake/pull/25) Support `NUMBER` type truly.

0.3.2 (2021-09-14)
==================

* [#23](https://github.com/trocco-io/embulk-output-snowflake/pull/23) Fix SnowflakeOutputPlugin location for [github.com/embulk/gradle-embulk-plugins](https://github.com/embulk/gradle-embulk-plugins).
* [#22](https://github.com/trocco-io/embulk-output-snowflake/pull/22) Fix a date format bug.

0.3.1 (2021-09-09)
==================

* Use [github.com/embulk/gradle-embulk-plugins](https://github.com/embulk/gradle-embulk-plugins) instead of old style gradle tasks.
* Upgrade Gradle 4.1 -> 6.9.1
* Add lockfile to lock dependencies.
* Add [CHANGELOG.md](./CHANGELOG.md).
    * There is no CHANGELOG for versions prior to 0.3.0.
* [#17](https://github.com/trocco-io/embulk-output-snowflake/pull/17) Automate gem publishing workflow
* [#18](https://github.com/trocco-io/embulk-output-snowflake/pull/18) Use spotless as a linter
* [#18](https://github.com/trocco-io/embulk-output-snowflake/pull/18) Use [google-java-format](https://github.com/google/google-java-format) instead of [airlift style](https://github.com/airlift/codestyle).

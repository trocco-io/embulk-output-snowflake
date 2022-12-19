build: gradlew/build
gem: gradlew/gem
check: gradlew/check
lint-auto: gradlew/spotlessApply
test: gradlew/test
update-dependencies:
	./gradlew dependencies --write-locks
gradlew/%:
	./gradlew $(@F)

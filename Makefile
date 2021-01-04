version := 1.0.0-$(shell /bin/date '+%Y%m%d%H%M%S')

no-default:
	@echo "no default target"

build-jar:
	@mvn -f $(CURDIR)/pom.xml clean package

clean:
	@mvn -f $(CURDIR)/pom.xml clean -q

github: clean
	@git add .
	@git commit -m "$(shell /bin/date "+%F %T")"
	@git push

.PHONY: no-default build-jar clean github
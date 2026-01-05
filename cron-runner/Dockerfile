FROM alpine:3.19

RUN apk add --no-cache bash curl postgresql-client busybox-suid

RUN mkdir -p /scripts
COPY run_jobs.sh /scripts/run_jobs.sh
RUN chmod +x /scripts/run_jobs.sh

COPY crontab.txt /etc/crontabs/root

CMD ["crond", "-f", "-l", "2"]

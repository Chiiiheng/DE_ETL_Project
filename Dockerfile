FROM deltaio/delta-docker:0.8.1_2.3.0
# https://github.com/delta-io/delta-docs/blob/main/static/quickstart_docker/0.8.1_2.3.0/Dockerfile

USER root

COPY ./requirements.txt ./
RUN pip install -r requirements.txt

# Set the working directory
WORKDIR /opt/spark/work-dir/

# Install cron
RUN apt-get update && apt-get install -y cron

# Add crontab file in the cron directory
COPY stock-etl-cron /etc/cron.d/stock-etl-cron

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/stock-etl-cron

# Apply cron job
RUN crontab /etc/cron.d/stock-etl-cron

# Copy the run script
COPY setup.sh /usr/src/app/setup.sh

# Give execution rights to the run script
RUN chmod +x /usr/src/app/setup.sh

# Create the log file to be able to run tail
RUN touch /var/log/cron.log

# Run cron
ENTRYPOINT ["/usr/src/app/setup.sh"]
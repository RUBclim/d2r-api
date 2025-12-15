# Maintenance

This chapter describes a few maintenance tasks that are needed to operate the network an
API.

## Managing deployments

The database, especially the metadata therein, is structured in a way that we have:

1. a collection of sensor that are known to belong to the network. They consists of
   different sensor types (blg, sht35, atm41)
1. a collections of stations (geographical sites) the network consists of
1. sensor deployments that combine the first two and assign (multiple) sensors to a
   station for a specific time span

Some changes to the network require manual changes to the metadata in the database via
SQL.

### Swapping a sensor

If a sensor is swapped for maintenance or after suffering damage due to vandalism or
construction the following steps are needed.

1. Terminate the old sensor's (`DEC0054AD`) deployment by:

   locating the associated deployment via the Decentlab sensor id

   ```sql
   SELECT * FROM sensor_deployment WHERE sensor_id = 'DEC0054AD';
   ```

   which will return something like this:

   ```console
    deployment_id | sensor_id | station_id |       setup_date       | teardown_date
   ---------------+-----------+------------+------------------------+---------------
               51 | DEC0054AD | DOTKPS     | 2024-08-15 23:59:00+00 |
   ```

   and terminating the deployment by setting a `teardown_date` for the `deployment_id`
   (in this case 51). The date should correspond to the last known valid measurement. If
   the old station has already been completely removed from the Element platform, you
   will have to set the `teardown_date` exactly at or before the last valid measurement.
   Otherwise the system will still try to download data to fill the gap between the last
   existing measurement and the `teardown_date` resulting in an error (`403`).

   ```sql
   UPDATE sensor_deployment SET teardown_date = '2025-11-28 05:40:00+00' WHERE deployment_id = 51;
   ```

2. Create a new deployment with the sensor that replaces the old sensor

   ```sql
   INSERT INTO
       sensor_deployment(sensor_id, station_id, setup_date)
   VALUES
       ('DEC004D18', 'DOTKPS', '2025-12-12 10:54')
   ```

3. You may need to clean up erroneous data that was recorded after the last valid
   measurement and before the beginning of the new deployment

   ```sql
   DELETE FROM temp_rh_data
   WHERE
       station_id = 'DOTKPS' AND
       measured_at BETWEEN '2025-11-28 05:41:00+00' AND '2025-12-12 10:54';
   ```

4. To also make the changes apply to the aggregated data (hourly and daily), you may
   simply wait a day (until `01:03:00 UTC`) when a routine job is started, fully
   refreshing all views. If the changes must apply immediately, you may start the task
   manually by running on the container host:

   ```bash
   docker exec -it celery celery -A app.tasks call refresh-all-views
   ```

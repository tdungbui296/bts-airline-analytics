with expected as (

    select
        date_trunc(flight_date, month) as flight_month,
        reporting_airline_dot_id,

        count(*) as scheduled_flights,

        countif(
            not operational_cancelled_flag
        ) as operated_flights,

        countif(
            operational_cancelled_flag
        ) as cancelled_flights,

        countif(
            is_diverted
        ) as diverted_flights,

        countif(
            departure_delay_minutes_signed is not null
        ) as departure_observed_flights,

        countif(
            arrival_delay_minutes_signed is not null
        ) as arrival_observed_flights,

        countif(
            is_departure_delayed_15_minutes
        ) as departure_delayed_15_flights,

        countif(
            is_arrival_delayed_15_minutes
        ) as arrival_delayed_15_flights,

        count(distinct flight_date)
            as active_service_days,

        count(distinct origin_airport_id)
            as origin_airport_count,

        count(distinct destination_airport_id)
            as destination_airport_count,

        count(
            distinct concat(
                cast(origin_airport_id as string),
                '->',
                cast(destination_airport_id as string)
            )
        ) as route_count,

        sum(
            coalesce(
                greatest(departure_delay_minutes_signed, 0),
                0
            )
        ) as total_departure_delay_minutes,

        sum(
            coalesce(
                greatest(arrival_delay_minutes_signed, 0),
                0
            )
        ) as total_arrival_delay_minutes,

        countif(
            valid_scheduled_elapsed_minutes is not null
        ) as scheduled_duration_eligible_flights,

        sum(
            coalesce(valid_scheduled_elapsed_minutes, 0)
        ) as total_scheduled_elapsed_minutes,

        countif(
            is_eligible_for_actual_duration_metrics
            and actual_elapsed_minutes is not null
        ) as actual_duration_eligible_flights,

        sum(
            case
                when is_eligible_for_actual_duration_metrics
                    then coalesce(actual_elapsed_minutes, 0)
                else 0
            end
        ) as total_actual_elapsed_minutes,

        countif(
            is_eligible_for_actual_duration_metrics
            and air_time_minutes is not null
        ) as air_time_observed_flights,

        sum(
            case
                when is_eligible_for_actual_duration_metrics
                    then coalesce(air_time_minutes, 0)
                else 0
            end
        ) as total_air_time_minutes,

        countif(
            taxi_out_minutes is not null
        ) as taxi_out_observed_flights,

        sum(
            coalesce(taxi_out_minutes, 0)
        ) as total_taxi_out_minutes,

        countif(
            taxi_in_minutes is not null
        ) as taxi_in_observed_flights,

        sum(
            coalesce(taxi_in_minutes, 0)
        ) as total_taxi_in_minutes,

        countif(
            distance_miles is not null
        ) as distance_observed_flights,

        sum(
            coalesce(distance_miles, 0)
        ) as total_distance_miles

    from {{ ref('fact_flights') }}

    group by
        flight_month,
        reporting_airline_dot_id

),

actual as (

    select
        flight_month,
        reporting_airline_dot_id,

        scheduled_flights,
        operated_flights,
        cancelled_flights,
        diverted_flights,

        departure_observed_flights,
        arrival_observed_flights,

        departure_delayed_15_flights,
        arrival_delayed_15_flights,

        active_service_days,
        origin_airport_count,
        destination_airport_count,
        route_count,

        total_departure_delay_minutes,
        total_arrival_delay_minutes,

        scheduled_duration_eligible_flights,
        total_scheduled_elapsed_minutes,

        actual_duration_eligible_flights,
        total_actual_elapsed_minutes,

        air_time_observed_flights,
        total_air_time_minutes,

        taxi_out_observed_flights,
        total_taxi_out_minutes,

        taxi_in_observed_flights,
        total_taxi_in_minutes,

        distance_observed_flights,
        total_distance_miles

    from {{ ref('mart_carrier_monthly_performance') }}

),

differences as (

    (
        select *
        from expected

        except distinct

        select *
        from actual
    )

    union all

    (
        select *
        from actual

        except distinct

        select *
        from expected
    )

)

select *
from differences
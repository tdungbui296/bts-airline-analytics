with source as (

    select *
    from {{ source('raw_bts', 'flights') }}

),

renamed as (

    select
        Year as flight_year,
        Month as flight_month,
        FlightDate as flight_date,

        Reporting_Airline as reporting_airline_code,
        DOT_ID_Reporting_Airline as reporting_airline_dot_id,
        Tail_Number as tail_number,
        Flight_Number_Reporting_Airline as flight_number,

        OriginAirportID as origin_airport_id,
        OriginAirportSeqID as origin_airport_sequence_id,
        OriginCityMarketID as origin_city_market_id,
        Origin as origin_airport_code,
        OriginCityName as origin_city_name,
        OriginState as origin_state_code,

        DestAirportID as destination_airport_id,
        DestAirportSeqID as destination_airport_sequence_id,
        DestCityMarketID as destination_city_market_id,
        Dest as destination_airport_code,
        DestCityName as destination_city_name,
        DestState as destination_state_code,

        CRSDepTime as scheduled_departure_hhmm,
        DepTime as actual_departure_hhmm,
        DepDelay as departure_delay_minutes_signed,
        DepDelayMinutes as departure_delay_minutes,
        DepDel15 as departure_delay_15_flag,
        DepartureDelayGroups as departure_delay_group,
        DepTimeBlk as departure_time_block,

        TaxiOut as taxi_out_minutes,
        WheelsOff as wheels_off_hhmm,
        WheelsOn as wheels_on_hhmm,
        TaxiIn as taxi_in_minutes,

        CRSArrTime as scheduled_arrival_hhmm,
        ArrTime as actual_arrival_hhmm,
        ArrDelay as arrival_delay_minutes_signed,
        ArrDelayMinutes as arrival_delay_minutes,
        ArrDel15 as arrival_delay_15_flag,
        ArrivalDelayGroups as arrival_delay_group,
        ArrTimeBlk as arrival_time_block,

        Cancelled as cancelled_flag,
        CancellationCode as cancellation_code,
        Diverted as diverted_flag,
        DivAirportLandings as diversion_airport_landings,

        CRSElapsedTime as scheduled_elapsed_minutes,
        ActualElapsedTime as actual_elapsed_minutes,
        AirTime as air_time_minutes,

        Distance as distance_miles,
        DistanceGroup as distance_group,

        CarrierDelay as carrier_delay_minutes,
        WeatherDelay as weather_delay_minutes,
        NASDelay as national_airspace_delay_minutes,
        SecurityDelay as security_delay_minutes,
        LateAircraftDelay as late_aircraft_delay_minutes,

        Cancelled = 1 as is_cancelled,
        Diverted = 1 as is_diverted,
        DepDel15 = 1 as is_departure_delayed_15_minutes,
        ArrDel15 = 1 as is_arrival_delayed_15_minutes,

        CRSElapsedTime is null
            as is_scheduled_elapsed_time_missing,

        CRSElapsedTime < 0
            as has_negative_scheduled_elapsed_time,

                Flight_Number_Reporting_Airline is null
            and FlightDate is not null
            and DOT_ID_Reporting_Airline is not null
            and Tail_Number is not null
            and OriginAirportID is not null
            and DestAirportID is not null
            and CRSDepTime is not null
            and CRSArrTime is not null
            as uses_fallback_flight_key,

        case
            when FlightDate is null
                or DOT_ID_Reporting_Airline is null
                or OriginAirportID is null
                or DestAirportID is null
                or CRSDepTime is null
                then null

            when Flight_Number_Reporting_Airline is not null
                then to_hex(
                    sha256(
                        concat(
                            cast(FlightDate as string),
                            '|',
                            cast(
                                DOT_ID_Reporting_Airline
                                as string
                            ),
                            '|',
                            cast(
                                Flight_Number_Reporting_Airline
                                as string
                            ),
                            '|',
                            cast(OriginAirportID as string),
                            '|',
                            cast(DestAirportID as string),
                            '|',
                            cast(CRSDepTime as string)
                        )
                    )
                )

            when Tail_Number is not null
                and CRSArrTime is not null
                then to_hex(
                    sha256(
                        concat(
                            'fallback|',
                            cast(FlightDate as string),
                            '|',
                            cast(
                                DOT_ID_Reporting_Airline
                                as string
                            ),
                            '|',
                            cast(Tail_Number as string),
                            '|',
                            cast(OriginAirportID as string),
                            '|',
                            cast(DestAirportID as string),
                            '|',
                            cast(CRSDepTime as string),
                            '|',
                            cast(CRSArrTime as string)
                        )
                    )
                )

            else null
        end as flight_key

    from source

)

select *
from renamed
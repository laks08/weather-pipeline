#!/usr/bin/env python3
"""
Mock data fixtures for NWS API responses.
Contains realistic mock responses for testing NWS integration.
"""

# Mock NWS Points API Response (Boston coordinates)
MOCK_NWS_POINTS_RESPONSE = {
    "@context": [
        "https://geojson.org/geojson-ld/geojson-context.jsonld",
        {
            "@version": "1.1",
            "wx": "https://api.weather.gov/ontology#",
            "s": "https://schema.org/",
            "geo": "http://www.opengis.net/ont/geosparql#",
            "unit": "http://codes.wmo.int/common/unit/",
            "@vocab": "https://api.weather.gov/ontology#",
            "geometry": {
                "@id": "s:GeoCoordinates",
                "@type": "geo:wktLiteral"
            },
            "city": "s:addressLocality",
            "state": "s:addressRegion",
            "distance": {
                "@id": "s:Distance",
                "@type": "s:QuantitativeValue"
            },
            "bearing": {
                "@type": "s:QuantitativeValue"
            },
            "value": {
                "@id": "s:value"
            },
            "unitCode": {
                "@id": "s:unitCode",
                "@type": "@id"
            },
            "forecastOffice": {
                "@type": "@id"
            },
            "forecastGridData": {
                "@type": "@id"
            },
            "publicZone": {
                "@type": "@id"
            },
            "county": {
                "@type": "@id"
            }
        }
    ],
    "id": "https://api.weather.gov/points/42.3601,-71.0589",
    "type": "Feature",
    "geometry": {
        "type": "Point",
        "coordinates": [
            -71.0589,
            42.3601
        ]
    },
    "properties": {
        "@id": "https://api.weather.gov/points/42.3601,-71.0589",
        "@type": "wx:Point",
        "cwa": "BOX",
        "forecastOffice": "https://api.weather.gov/offices/BOX",
        "gridId": "BOX",
        "gridX": 71,
        "gridY": 90,
        "forecast": "https://api.weather.gov/gridpoints/BOX/71,90/forecast",
        "forecastHourly": "https://api.weather.gov/gridpoints/BOX/71,90/forecast/hourly",
        "forecastGridData": "https://api.weather.gov/gridpoints/BOX/71,90",
        "observationStations": "https://api.weather.gov/gridpoints/BOX/71,90/stations",
        "relativeLocation": {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    -71.063611,
                    42.358431
                ]
            },
            "properties": {
                "city": "Boston",
                "state": "MA",
                "distance": {
                    "unitCode": "wmoUnit:m",
                    "value": 569.8521
                },
                "bearing": {
                    "unitCode": "wmoUnit:degree_(angle)",
                    "value": 123
                }
            }
        },
        "forecastZone": "https://api.weather.gov/zones/forecast/MAZ015",
        "county": "https://api.weather.gov/zones/county/MAC025",
        "fireWeatherZone": "https://api.weather.gov/zones/fire/MAZ015",
        "timeZone": "America/New_York",
        "radarStation": "KBOX"
    }
}

# Mock NWS Observation Stations Response
MOCK_NWS_STATIONS_RESPONSE = {
    "@context": {
        "@version": "1.1",
        "wx": "https://api.weather.gov/ontology#",
        "s": "https://schema.org/",
        "geo": "http://www.opengis.net/ont/geosparql#",
        "unit": "http://codes.wmo.int/common/unit/",
        "@vocab": "https://api.weather.gov/ontology#"
    },
    "type": "FeatureCollection",
    "features": [
        {
            "id": "https://api.weather.gov/stations/KBOS",
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    -71.005,
                    42.3647
                ]
            },
            "properties": {
                "@id": "https://api.weather.gov/stations/KBOS",
                "@type": "wx:ObservationStation",
                "elevation": {
                    "unitCode": "wmoUnit:m",
                    "value": 6.0960
                },
                "stationIdentifier": "KBOS",
                "name": "Boston, Logan International Airport",
                "timeZone": "America/New_York"
            }
        },
        {
            "id": "https://api.weather.gov/stations/KMQE",
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [
                    -71.0275,
                    42.3408
                ]
            },
            "properties": {
                "@id": "https://api.weather.gov/stations/KMQE",
                "@type": "wx:ObservationStation",
                "elevation": {
                    "unitCode": "wmoUnit:m",
                    "value": 3.9624
                },
                "stationIdentifier": "KMQE",
                "name": "Boston, Boston City Heliport",
                "timeZone": "America/New_York"
            }
        }
    ],
    "observationStations": [
        "https://api.weather.gov/stations/KBOS",
        "https://api.weather.gov/stations/KMQE"
    ]
}

# Mock NWS Current Conditions Response
MOCK_NWS_CURRENT_RESPONSE = {
    "@context": {
        "@version": "1.1",
        "wx": "https://api.weather.gov/ontology#",
        "s": "https://schema.org/",
        "geo": "http://www.opengis.net/ont/geosparql#",
        "unit": "http://codes.wmo.int/common/unit/",
        "@vocab": "https://api.weather.gov/ontology#"
    },
    "id": "https://api.weather.gov/stations/KBOS/observations/2024-01-15T10:51:00+00:00",
    "type": "Feature",
    "geometry": {
        "type": "Point",
        "coordinates": [
            -71.005,
            42.3647
        ]
    },
    "properties": {
        "@id": "https://api.weather.gov/stations/KBOS/observations/2024-01-15T10:51:00+00:00",
        "@type": "wx:ObservationStation",
        "elevation": {
            "unitCode": "wmoUnit:m",
            "value": 6.0960
        },
        "station": "https://api.weather.gov/stations/KBOS",
        "timestamp": "2024-01-15T10:51:00+00:00",
        "rawMessage": "METAR KBOS 151051Z 28008KT 10SM FEW250 05/M08 A3015 RMK AO2 SLP210 T00501083",
        "textDescription": "Fair",
        "icon": "https://api.weather.gov/icons/land/day/few?size=medium",
        "presentWeather": [],
        "temperature": {
            "unitCode": "wmoUnit:degC",
            "value": 5.0,
            "qualityControl": "V"
        },
        "dewpoint": {
            "unitCode": "wmoUnit:degC",
            "value": -8.3,
            "qualityControl": "V"
        },
        "windDirection": {
            "unitCode": "wmoUnit:degree_(angle)",
            "value": 280,
            "qualityControl": "V"
        },
        "windSpeed": {
            "unitCode": "wmoUnit:m_s-1",
            "value": 4.12,
            "qualityControl": "V"
        },
        "windGust": {
            "unitCode": "wmoUnit:m_s-1",
            "value": None,
            "qualityControl": "Z"
        },
        "barometricPressure": {
            "unitCode": "wmoUnit:Pa",
            "value": 102050,
            "qualityControl": "V"
        },
        "seaLevelPressure": {
            "unitCode": "wmoUnit:Pa",
            "value": 102100,
            "qualityControl": "V"
        },
        "visibility": {
            "unitCode": "wmoUnit:m",
            "value": 16090,
            "qualityControl": "C"
        },
        "maxTemperatureLast24Hours": {
            "unitCode": "wmoUnit:degC",
            "value": None
        },
        "minTemperatureLast24Hours": {
            "unitCode": "wmoUnit:degC",
            "value": None
        },
        "precipitationLastHour": {
            "unitCode": "wmoUnit:mm",
            "value": None,
            "qualityControl": "Z"
        },
        "precipitationLast3Hours": {
            "unitCode": "wmoUnit:mm",
            "value": None,
            "qualityControl": "Z"
        },
        "precipitationLast6Hours": {
            "unitCode": "wmoUnit:mm",
            "value": None,
            "qualityControl": "Z"
        },
        "relativeHumidity": {
            "unitCode": "wmoUnit:percent",
            "value": 45.8,
            "qualityControl": "V"
        },
        "windChill": {
            "unitCode": "wmoUnit:degC",
            "value": 1.1,
            "qualityControl": "V"
        },
        "heatIndex": {
            "unitCode": "wmoUnit:degC",
            "value": None,
            "qualityControl": "V"
        },
        "cloudLayers": [
            {
                "base": {
                    "unitCode": "wmoUnit:m",
                    "value": 7620
                },
                "amount": "FEW"
            }
        ]
    }
}

# Mock NWS Hourly Forecast Response
MOCK_NWS_HOURLY_RESPONSE = {
    "@context": [
        "https://geojson.org/geojson-ld/geojson-context.jsonld",
        {
            "@version": "1.1",
            "wx": "https://api.weather.gov/ontology#",
            "geo": "http://www.opengis.net/ont/geosparql#",
            "unit": "http://codes.wmo.int/common/unit/",
            "@vocab": "https://api.weather.gov/ontology#"
        }
    ],
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-71.0617676, 42.3584851],
                [-71.0617676, 42.3617849],
                [-71.0560324, 42.3617849],
                [-71.0560324, 42.3584851],
                [-71.0617676, 42.3584851]
            ]
        ]
    },
    "properties": {
        "updated": "2024-01-15T09:15:45+00:00",
        "units": "us",
        "forecastGenerator": "HourlyForecastGenerator",
        "generatedAt": "2024-01-15T10:52:31+00:00",
        "updateTime": "2024-01-15T09:15:45+00:00",
        "validTimes": "2024-01-15T10:00:00+00:00/P7DT14H",
        "elevation": {
            "unitCode": "wmoUnit:m",
            "value": 43.8912
        },
        "periods": [
            {
                "number": 1,
                "name": "This Hour",
                "startTime": "2024-01-15T10:00:00-05:00",
                "endTime": "2024-01-15T11:00:00-05:00",
                "isDaytime": True,
                "temperature": 41,
                "temperatureUnit": "F",
                "temperatureTrend": None,
                "probabilityOfPrecipitation": {
                    "unitCode": "wmoUnit:percent",
                    "value": 0
                },
                "dewpoint": {
                    "unitCode": "wmoUnit:degF",
                    "value": 17
                },
                "relativeHumidity": {
                    "unitCode": "wmoUnit:percent",
                    "value": 46
                },
                "windSpeed": "9 mph",
                "windDirection": "W",
                "icon": "https://api.weather.gov/icons/land/day/few?size=small",
                "shortForecast": "Sunny",
                "detailedForecast": ""
            },
            {
                "number": 2,
                "name": "",
                "startTime": "2024-01-15T11:00:00-05:00",
                "endTime": "2024-01-15T12:00:00-05:00",
                "isDaytime": True,
                "temperature": 43,
                "temperatureUnit": "F",
                "temperatureTrend": None,
                "probabilityOfPrecipitation": {
                    "unitCode": "wmoUnit:percent",
                    "value": 0
                },
                "dewpoint": {
                    "unitCode": "wmoUnit:degF",
                    "value": 17
                },
                "relativeHumidity": {
                    "unitCode": "wmoUnit:percent",
                    "value": 43
                },
                "windSpeed": "10 mph",
                "windDirection": "W",
                "icon": "https://api.weather.gov/icons/land/day/few?size=small",
                "shortForecast": "Sunny",
                "detailedForecast": ""
            },
            {
                "number": 3,
                "name": "",
                "startTime": "2024-01-15T12:00:00-05:00",
                "endTime": "2024-01-15T13:00:00-05:00",
                "isDaytime": True,
                "temperature": 45,
                "temperatureUnit": "F",
                "temperatureTrend": None,
                "probabilityOfPrecipitation": {
                    "unitCode": "wmoUnit:percent",
                    "value": 0
                },
                "dewpoint": {
                    "unitCode": "wmoUnit:degF",
                    "value": 17
                },
                "relativeHumidity": {
                    "unitCode": "wmoUnit:percent",
                    "value": 40
                },
                "windSpeed": "10 mph",
                "windDirection": "W",
                "icon": "https://api.weather.gov/icons/land/day/few?size=small",
                "shortForecast": "Sunny",
                "detailedForecast": ""
            }
        ]
    }
}

# Mock NWS Daily Forecast Response
MOCK_NWS_DAILY_RESPONSE = {
    "@context": [
        "https://geojson.org/geojson-ld/geojson-context.jsonld",
        {
            "@version": "1.1",
            "wx": "https://api.weather.gov/ontology#",
            "geo": "http://www.opengis.net/ont/geosparql#",
            "unit": "http://codes.wmo.int/common/unit/",
            "@vocab": "https://api.weather.gov/ontology#"
        }
    ],
    "type": "Feature",
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-71.0617676, 42.3584851],
                [-71.0617676, 42.3617849],
                [-71.0560324, 42.3617849],
                [-71.0560324, 42.3584851],
                [-71.0617676, 42.3584851]
            ]
        ]
    },
    "properties": {
        "updated": "2024-01-15T09:15:45+00:00",
        "units": "us",
        "forecastGenerator": "BaselineForecastGenerator",
        "generatedAt": "2024-01-15T10:52:31+00:00",
        "updateTime": "2024-01-15T09:15:45+00:00",
        "validTimes": "2024-01-15T09:00:00+00:00/P8D",
        "elevation": {
            "unitCode": "wmoUnit:m",
            "value": 43.8912
        },
        "periods": [
            {
                "number": 1,
                "name": "Today",
                "startTime": "2024-01-15T06:00:00-05:00",
                "endTime": "2024-01-15T18:00:00-05:00",
                "isDaytime": True,
                "temperature": 47,
                "temperatureUnit": "F",
                "temperatureTrend": None,
                "probabilityOfPrecipitation": {
                    "unitCode": "wmoUnit:percent",
                    "value": 0
                },
                "dewpoint": {
                    "unitCode": "wmoUnit:degF",
                    "value": 17
                },
                "relativeHumidity": {
                    "unitCode": "wmoUnit:percent",
                    "value": 40
                },
                "windSpeed": "9 to 13 mph",
                "windDirection": "W",
                "icon": "https://api.weather.gov/icons/land/day/few?size=medium",
                "shortForecast": "Sunny",
                "detailedForecast": "Sunny, with a high near 47. West wind 9 to 13 mph."
            },
            {
                "number": 2,
                "name": "Tonight",
                "startTime": "2024-01-15T18:00:00-05:00",
                "endTime": "2024-01-16T06:00:00-05:00",
                "isDaytime": False,
                "temperature": 32,
                "temperatureUnit": "F",
                "temperatureTrend": None,
                "probabilityOfPrecipitation": {
                    "unitCode": "wmoUnit:percent",
                    "value": 0
                },
                "dewpoint": {
                    "unitCode": "wmoUnit:degF",
                    "value": 17
                },
                "relativeHumidity": {
                    "unitCode": "wmoUnit:percent",
                    "value": 55
                },
                "windSpeed": "6 to 9 mph",
                "windDirection": "W",
                "icon": "https://api.weather.gov/icons/land/night/few?size=medium",
                "shortForecast": "Mostly Clear",
                "detailedForecast": "Mostly clear, with a low around 32. West wind 6 to 9 mph."
            },
            {
                "number": 3,
                "name": "Tuesday",
                "startTime": "2024-01-16T06:00:00-05:00",
                "endTime": "2024-01-16T18:00:00-05:00",
                "isDaytime": True,
                "temperature": 50,
                "temperatureUnit": "F",
                "temperatureTrend": None,
                "probabilityOfPrecipitation": {
                    "unitCode": "wmoUnit:percent",
                    "value": 0
                },
                "dewpoint": {
                    "unitCode": "wmoUnit:degF",
                    "value": 20
                },
                "relativeHumidity": {
                    "unitCode": "wmoUnit:percent",
                    "value": 45
                },
                "windSpeed": "6 to 10 mph",
                "windDirection": "SW",
                "icon": "https://api.weather.gov/icons/land/day/few?size=medium",
                "shortForecast": "Sunny",
                "detailedForecast": "Sunny, with a high near 50. Southwest wind 6 to 10 mph."
            },
            {
                "number": 4,
                "name": "Tuesday Night",
                "startTime": "2024-01-16T18:00:00-05:00",
                "endTime": "2024-01-17T06:00:00-05:00",
                "isDaytime": False,
                "temperature": 35,
                "temperatureUnit": "F",
                "temperatureTrend": None,
                "probabilityOfPrecipitation": {
                    "unitCode": "wmoUnit:percent",
                    "value": 20
                },
                "dewpoint": {
                    "unitCode": "wmoUnit:degF",
                    "value": 25
                },
                "relativeHumidity": {
                    "unitCode": "wmoUnit:percent",
                    "value": 65
                },
                "windSpeed": "5 to 8 mph",
                "windDirection": "S",
                "icon": "https://api.weather.gov/icons/land/night/sct?size=medium",
                "shortForecast": "Partly Cloudy",
                "detailedForecast": "Partly cloudy, with a low around 35. South wind 5 to 8 mph."
            }
        ]
    }
}

# Mock Error Responses
MOCK_NWS_404_RESPONSE = {
    "correlationId": "12345678-1234-1234-1234-123456789012",
    "title": "Data Unavailable For Requested Point",
    "type": "https://api.weather.gov/problems/InvalidPoint",
    "status": 404,
    "detail": "Unable to provide data for requested point.",
    "instance": "https://api.weather.gov/requests/12345678-1234-1234-1234-123456789012"
}

MOCK_NWS_503_RESPONSE = {
    "correlationId": "12345678-1234-1234-1234-123456789012",
    "title": "Service Unavailable",
    "type": "https://api.weather.gov/problems/ServiceUnavailable",
    "status": 503,
    "detail": "The service is temporarily unavailable. Please try again later.",
    "instance": "https://api.weather.gov/requests/12345678-1234-1234-1234-123456789012"
}

# Test data for different weather conditions
MOCK_WEATHER_CONDITIONS = {
    "clear": {
        "properties": {
            "timestamp": "2024-01-15T12:00:00+00:00",
            "temperature": {"value": 20.0, "unitCode": "wmoUnit:degC"},
            "relativeHumidity": {"value": 50},
            "barometricPressure": {"value": 101325, "unitCode": "wmoUnit:Pa"},
            "windSpeed": {"value": 5.0, "unitCode": "wmoUnit:m_s-1"},
            "windDirection": {"value": 180},
            "textDescription": "Clear"
        }
    },
    "rainy": {
        "properties": {
            "timestamp": "2024-01-15T12:00:00+00:00",
            "temperature": {"value": 15.0, "unitCode": "wmoUnit:degC"},
            "relativeHumidity": {"value": 85},
            "barometricPressure": {"value": 100800, "unitCode": "wmoUnit:Pa"},
            "windSpeed": {"value": 8.0, "unitCode": "wmoUnit:m_s-1"},
            "windDirection": {"value": 225},
            "textDescription": "Light Rain"
        }
    },
    "snowy": {
        "properties": {
            "timestamp": "2024-01-15T12:00:00+00:00",
            "temperature": {"value": -2.0, "unitCode": "wmoUnit:degC"},
            "relativeHumidity": {"value": 90},
            "barometricPressure": {"value": 102000, "unitCode": "wmoUnit:Pa"},
            "windSpeed": {"value": 12.0, "unitCode": "wmoUnit:m_s-1"},
            "windDirection": {"value": 45},
            "textDescription": "Snow",
            "windChill": {"value": -8.0, "unitCode": "wmoUnit:degC"}
        }
    },
    "hot": {
        "properties": {
            "timestamp": "2024-07-15T12:00:00+00:00",
            "temperature": {"value": 35.0, "unitCode": "wmoUnit:degC"},
            "relativeHumidity": {"value": 60},
            "barometricPressure": {"value": 101200, "unitCode": "wmoUnit:Pa"},
            "windSpeed": {"value": 3.0, "unitCode": "wmoUnit:m_s-1"},
            "windDirection": {"value": 90},
            "textDescription": "Sunny",
            "heatIndex": {"value": 40.0, "unitCode": "wmoUnit:degC"}
        }
    }
}

# Edge case test data
MOCK_EDGE_CASES = {
    "missing_temperature": {
        "properties": {
            "timestamp": "2024-01-15T12:00:00+00:00",
            "relativeHumidity": {"value": 50},
            "textDescription": "Unknown"
        }
    },
    "invalid_units": {
        "properties": {
            "timestamp": "2024-01-15T12:00:00+00:00",
            "temperature": {"value": 20.0, "unitCode": "invalid:unit"},
            "barometricPressure": {"value": 101325, "unitCode": "invalid:pressure"},
            "windSpeed": {"value": 5.0, "unitCode": "invalid:speed"},
            "textDescription": "Test"
        }
    },
    "null_values": {
        "properties": {
            "timestamp": "2024-01-15T12:00:00+00:00",
            "temperature": {"value": None, "unitCode": "wmoUnit:degC"},
            "relativeHumidity": {"value": None},
            "barometricPressure": {"value": None, "unitCode": "wmoUnit:Pa"},
            "windSpeed": {"value": None, "unitCode": "wmoUnit:m_s-1"},
            "windDirection": {"value": None},
            "textDescription": None
        }
    }
}

def get_mock_response(response_type: str, condition: str = "clear"):
    """
    Get a mock response for testing.
    
    Args:
        response_type: Type of response ('points', 'stations', 'current', 'hourly', 'daily', 'error')
        condition: Weather condition for current responses ('clear', 'rainy', 'snowy', 'hot')
        
    Returns:
        Dict containing the mock response
    """
    if response_type == "points":
        return MOCK_NWS_POINTS_RESPONSE
    elif response_type == "stations":
        return MOCK_NWS_STATIONS_RESPONSE
    elif response_type == "current":
        if condition in MOCK_WEATHER_CONDITIONS:
            return MOCK_WEATHER_CONDITIONS[condition]
        return MOCK_NWS_CURRENT_RESPONSE
    elif response_type == "hourly":
        return MOCK_NWS_HOURLY_RESPONSE
    elif response_type == "daily":
        return MOCK_NWS_DAILY_RESPONSE
    elif response_type == "error_404":
        return MOCK_NWS_404_RESPONSE
    elif response_type == "error_503":
        return MOCK_NWS_503_RESPONSE
    else:
        raise ValueError(f"Unknown response type: {response_type}")


def get_edge_case_response(case: str):
    """
    Get an edge case response for testing.
    
    Args:
        case: Edge case type ('missing_temperature', 'invalid_units', 'null_values')
        
    Returns:
        Dict containing the edge case response
    """
    if case in MOCK_EDGE_CASES:
        return MOCK_EDGE_CASES[case]
    else:
        raise ValueError(f"Unknown edge case: {case}")


if __name__ == "__main__":
    # Test the mock data functions
    print("Testing mock data functions...")
    
    # Test basic responses
    points = get_mock_response("points")
    assert "properties" in points
    assert "forecast" in points["properties"]
    print("✓ Points response mock working")
    
    current = get_mock_response("current", "clear")
    assert "properties" in current
    assert "temperature" in current["properties"]
    print("✓ Current response mock working")
    
    # Test edge cases
    edge_case = get_edge_case_response("missing_temperature")
    assert "properties" in edge_case
    assert "temperature" not in edge_case["properties"]
    print("✓ Edge case mock working")
    
    print("🎉 All mock data functions working correctly!")
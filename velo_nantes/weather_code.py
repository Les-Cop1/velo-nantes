def code_to_string(code: str):
    if code is None:
        return None

    code = int(code)

    if code in [0]:
        return "clear"

    if code in [1, 2, 3]:
        return "cloudy"

    if code in [45, 48]:
        return "fog"

    if code in [51, 53, 55]:
        return "drizzle"

    if code in [56, 57]:
        return "freezing_drizzle"

    if code in [61, 63, 65]:
        return "rain"

    if code in [66, 67]:
        return "freezing_rain"

    if code in [71, 73, 75]:
        return "snow"

    if code in [77]:
        return "snow_grains"

    if code in [80, 81, 82]:
        return "rain_showers"

    if code in [85, 86]:
        return "snow_showers"

    if code in [95, 96, 99]:
        return "thunderstorm"

    return None

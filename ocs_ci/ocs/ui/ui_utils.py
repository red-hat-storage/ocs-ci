def format_locator(locator, string_to_insert):
    """
    Use this function format_locator when working with dynamic locators.

    Args:
        locator (tuple): (GUI element needs to operate on (str), type (By))
        string_to_insert (str): Name of the variable (string) which contains the dynamic web element
            when generated on certain action

    return:
        formats the locator using .format() function which takes string to be inserted as an argument

    """
    return locator[0].format(string_to_insert), locator[1]

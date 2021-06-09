# Coding guidelines for OCS-CI project

Let's follow the following guidelines for the Core Libraries and Test Scripts

* Follow pep8 or use autopep8 to automagically conform to pep8 style.
* Each Python Class is well documented for inheritance, methods and
    attributes. Add sufficient comments for code block that is easy
    to understand for others to review.
    Use [Google Style of Python Docstrings](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html)
* Code Readability takes priority, Lets avoid crazy hacks and tricks
    and stick to straight forward code blocks that are easy to
    understand for all.
* For core libraries when possible please add the unittest to
    minimize regression for many tests
* Helper functions that are useful to wide variety of tests should
    reside in utils and/or should be carefully reviewed to check if
    they can actually be methods.
* test_scripts will reside in the `tests/` folder under the correct subsection
 and will follow [pytest](https://docs.pytest.org/en/latest/) conventions.
* Do not use backslashes in the code for line breaker!
* Line length should be maximum 79 characters!
* Try to follow this convention for brackets and indentation like in example
    below if the line doesn't fit in 79 chars!  (This makes code much more
    readable):
* If you are introducing a workaround, follow rules noted in
  [Tracking of workarounds](./workarounds.md) page.
* We use [black](https://black.readthedocs.io/en/stable/index.html) in our pre-commit hooks
  to automatically format code before it hits code review. Our PR checks are also using
  black to verify that code that has made it to review complies with our guidelines. If
  black will take any action on the code (if reformatting is necessary) then the PR check
  will fail.

```python
def function(
    parameter1, parameter2, parameter3, parameter4, parameter5, parameter6,
    parameter7='Default value of param7'
):
    print(
        "Hello, I am long string which can be easily split like this"
        "without using backslashes!"
    )
    my_dict = {
        'key1': "Value is long and it looks better to have new line here",
        'another_key': 'Another value',  # please keep comma at the end!
    }
    if value >= 10:
        print("Doing something here")
        my_list = [
            variable for variable in range(100) if variable % 2 == 0 if
            variable % 5 == 0
        ]

        # If line can fit in 79 chares do it as one liner (No need to split)!
        my_list_fit_one_line = [x for x in range(10)]
```

* **String formatting**: use new style of string formatting:

```python
print("These are new style of Python formatting:")
"My string {}".format("value")
"My string {var_name}".format(var_name="value")
f"This is the best and preferred way {var_name}"  # from Python 3.6

print("This is old style of formatting and should be avoided:")
"My string %s" % variable
```

* **Docstring**:
* Use capital letters properly
* Every docstring should describing easily what the function/class/etc. do
* The description should be as follow:
```python
def my_example(arg1, arg2=10):
    """
    This function is for showing how a proper docstring should
    look like

    Args:
        arg1 (<type of argument>): Description of what this argument is and if
            the line is too long we will indent according to this (4 spaces)
        arg2 (int): This argument is an integer (default: 10)

    Raises:
        ExampleException: you get it

    Returns:
        bool: description of when returns False and when True.
            But this can also return anything else like int, str, list, etc.

    """
    pass
```

* **Logging**: let the logger format log message for you:

```python
logger.info("My message %s", variable)  # No usage of % after string!
# This is still under consideration. We can set the style of logger
# format to use {} instead %s which is possible from Python 3.2.
```

Here is the [Python documentation](https://docs.python.org/3/howto/logging-cookbook.html#use-of-alternative-formatting-styles)
for Logger Styles.

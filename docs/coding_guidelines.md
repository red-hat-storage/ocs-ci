# Coding guidelines for OCS-CI project

Lets follow the following guidelines for the Core Libraries and Test Scripts

* Follow pep8 or use autopep8 to automagically conform to pep8 style.
* Each Python Class is well documented for inheritence, methods and
    attributes, Add sufficent comments for code block that is easy
    to understand for others to review.
* Code Readablity takes priority, Lets avoid crazy hacks and tricks 
    and stick to straight forward code blocks that are easy to
    understand for all.
* For core libraries when possible please add the unittest to
    minimize regression for many tests
* Helper functions that are useful to wide variety of tests should
    reside in utils and/or should be carefully reviewed to check if
    they can actually be methods.
* test_scripts will reside in test/ folders and will define run 
    unction
* test_scripts should return non zero exit status for failure and 0
    for 'pass'

**ex: test_scripts/test_ocs_operator.py**

```python
        def run(ocs_context, **kw):
            # use ocs_context to deal with cluster
            # run your test steps
            if test_failed:
                return False
            else:
                return Pass
```

* Do not use backslashes in theh code for line breaker!
* Line length should be maximum 79 characters!
* Try to follow this convention for brackets and indentions like in example below (This makes code much more readable):

```python
def function(
    param1, param2, param3, param4
):
    print(
        "Hello, I am long string which can be easily splitted like "
        "this"
    )
    my_dict = {
        'key': "Value",
        'another_key': 'Another value',  # please keep comma at the end!
    }
    if value >= 10:
        print("Doing something here")
        my_list = [
            y for y in range(100) if y % 2 == 0 if y % 5 == 0
        ]
```

* **String formatting**: use new style of string formatting:

```python
print("These are new style of Python formatting:")
"My string {}".format("value")
"My string {var_name}".format(var_name="value")
f"This is the best and preffered way {var_name}"  # from Python 3.6

print("Thihs is old style of formatting and should be avoided:")
"My strin %s" % variable
```

* **Logging**: let the logger format log message for you:

```python
logger.info("My message %s", variable)  # No usage of % after string! 
# This is still under consideration. We can set the style of logger
# format to use {} instead %s which is possible from Python 3.2.
```

Here is the [Python documentation](https://docs.python.org/3/howto/logging-cookbook.html#use-of-alternative-formatting-styles)
for Logger Styles.
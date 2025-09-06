# Import necessary modules for the function
import json
import functools

# The function itself, in which event is the full request sent to the API
# the keyword event is a json string, which has been sent from snowflake
# to the lambda function in the form of a json string via the gateway api
# from the snowflake external function, 'event' is therefore the main parameter
# as being a json dataset definition sent from snowflake as an array
# and converted into a json data string and passed into the event parameter
def lambda_handler(event, context):
    # Declare return variables
    # default api message sending status to 200 which means 'success'
    statusCode = 200
    # dataArray stores the resultset of the function which will be
    # returned by the function. This begins as an empty array and is added
    # to in the rows loop below
    # each sum result is an array, therefore datarray will actually hold as results an
    # array of 'sum result' arrays, so its actually an array of arrays (a 2D array)
    dataArray = []

    # json_compatible_string_to_return is the json body returned by the
    # function, stored as a string. This empty value is replaced by the real
    # result when ready
    json_compatible_string_to_return = ''

    # try/except is used for error handling
    try:
        # Retrieve the body of the request as a JSON object
        body = json.loads(event['body'])

        # Retrieve the 'data' key of the request body
        # When the request comes from Snowflake, we expect data to be a
        # an array of each row in the Snowflake query resultset.
        # Each row is its own array, which begins with the row number,
        # for example [0, 2, 3] would be the 0th row, in which the
        # variables passed to the function are 2 and 3. In this case,
        # the function sum output would be 5.
        rows = body['data']

        # Loop through each row
        for row in rows:

            # Retrieve the row number from the start of the row array
            rowNumber = row[0]

            # Retrieve the array of numbers to sum
            numbersToSum = row[1:]

            # try/except is used for error handling
            try:
                # Calculate the rowSum
                rowSum = functools.reduce(lambda a,b : a+b, numbersToSum)
            except:
                rowSum = "Error"

            # Create a new array entry
            newArrayEntry = [rowNumber, rowSum]

            # Add the newArrayEntry to the main dataArray list
            dataArray.append(newArrayEntry)

        # Put dataArray into a dictionary, then convert it to a string
        dataArrayToReturn = {'data' : dataArray}
        json_compatible_string_to_return = json.dumps(dataArrayToReturn)

    except Exception as err:
        # Statuscode = 400 signifies an error
        statusCode = 400
        # Function will return the error
        json_compatible_string_to_return = json.dumps({"data":str(err)})

    return {
        'statusCode': statusCode
        ,   'headers': { 'Content-Type': 'application/json' }
        ,   'body' : json_compatible_string_to_return
        }

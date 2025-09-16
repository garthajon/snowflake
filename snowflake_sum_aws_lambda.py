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
    # default api message setting status to 200 which means 'success'
    # this is a HTTP standard code for 'okay'/success used by the api gateway/rest apis as standard
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
        # converts the json string from the event parameter into a json object called body
        body = json.loads(event['body'])

        # Retrieve the 'data' key of the request body
        # When the request comes from Snowflake, we expect data to be a
        # an array of each row in the Snowflake query resultset.
        # Each row is its own array, which begins with the row number,
        # for example [0, 2, 3] would be the 0th row, in which the
        # variables passed to the function are 2 and 3. In this case,
        # the function sum output would be 5.
        # extract the data array as rows from the body json object
        rows = body['data']

        # Loop through each row from the array of rows extracted from the json object
        # in the example each row is a set of numbers to be summed
        for row in rows:

            # Retrieve the row number from the start of the row array
            # the first element of the row array is the row number
            rowNumber = row[0]

            # Retrieve the array of numbers to sum
            # all array items after the first element are the numbers to sum
            numbersToSum = row[1:]

            # try/except is used for error handling
            try:
                # Calculate the rowSum
                # the reduce function iterates through the numbersToSum array cumulatively adding as it goes
                rowSum = functools.reduce(lambda a,b : a+b, numbersToSum)
            except:
                rowSum = "Error"

            # Create a new array entry
            # create a new one dimensional array with the row number and the sum result
            # for this row
            newArrayEntry = [rowNumber, rowSum]
         

            # Add the newArrayEntry to the main dataArray list
            # add the current one dimensional array to the main dataArray as sum result
            dataArray.append(newArrayEntry)

            # for loop continues to the next row here

        
        # for loop finishes here with fully populated dataArray
        # Put dataArray into a dictionary, then convert it into a json format string
        dataArrayToReturn = {"data" : dataArray}
        json_compatible_string_to_return = json.dumps(dataArrayToReturn)
        

           

    except Exception as err:
        # Statuscode = 400 signifies an error
        # this is a HTTP standard code for 'bad request'/error used by the api gateway/rest apis as standard
        statusCode = 400
        # Function will return the error
        json_compatible_string_to_return = json.dumps({"data":str(err)})

    
    return {
        'statusCode': statusCode
        ,   'headers': { 'Content-Type': 'application/json' }
        ,   'body' : json_compatible_string_to_return
        }
   
    
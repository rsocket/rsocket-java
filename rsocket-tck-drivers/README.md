# RSocket TCK Drivers

This is meant to be used in conjunction with the TCK at [reactivesocket-tck](https://github.com/RSocket/reactivesocket-tck)

## Basic Idea and Organization

The philosophy behind the TCK is that it should allow any implementation of RSocket to verify itself against any other
implementation. In order to provide a truly polyglot solution, we determined that the best way to do so was to provide a central
TCK repository with only the code that generates the intermediate script, and then leave it up to implementers to create the
drivers for their own implementation. The script was created specifically to be easy to parse and implement drivers for,
and this Java driver is the first driver to be created as the Java implementation of RSockets is the most mature at
the time.

The driver is organized with a simple structure. On both the client and server drivers, we have the main driver class that
do an intial parse of the script files. On the server side, this process basically constructs dynamic request handlers where
every time a request is received, the appropriate behavior is searched up and is passed to a ParseMarble object, which is run
on its own thread and is used to parse through a marble diagram and enact it's behavior. On the client side, the main driver
class splits up each test into it's own individual lines, and then runs each test synchronously in its own thread. Support
for concurrent behavior can easily be added later.

On the client side, for the most part, each test thread just parses through each line of the test in order, synchronously and enacts its
behavior on our TestSubscriber, a special subscriber that we can use to verify that certain things have happened. `await` calls
should block the main test thread, and the test should fail if a single assert fails.

Things get trickier with channel tests, because the client and server need to have the same behavior. In channel tests on both
sides, the driver creates a ParseChannel object, which parses through the contents of a channel tests and handles receiving
and sending data. We use the ParseMarble object to handle sending data. Here, we have one thread that continuously runs `parse()`,
and other threads that run `add()` and `request()`, which stages data to be added and requested.



## Run Instructions

You can build the project with `gradle build`.
You can run the client and server using the `run` script with `./run [options]`. The options are:

`--server` : This is if you want to launch the server

`--client` : This is if you want to launch the client

`--host <h>` : This is for the client only, determines what host to connect to

`--port <p>` : If launching as client, tells it to connect to port `p`, and if launching as server, tells what port to server on

`--file <f>` : The path to the script file. Make sure to give the server and client the correct file formats

`--debug` : This is if you want to look at the individual frames being sent and received by the client

`--tests` : This allows you, when you're running the client, to specify the tests you want to run by name. Each test
should be comma separated.

Examples:
`./run.sh --server --port 4567 --file src/test/resources/servertest.txt` should start up a server  on port `4567` that
has its behavior determined by the file `servertest.txt`.

`./run.sh --client --host localhost --port 4567 --file src/test/resources/clienttest.txt --debug --tests genericTest,badTest` should
start the client and have it connect to localhost on port `4567` and load the tests in `clienttest.txt` in debug mode,
and only run the tests named `genericTest` and `badTest`.


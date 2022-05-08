## Tasman - Data Engineering Task

To activate the virtual environment, run the following command:

    $ pipenv shell

The pipfile is located in the root of the project.

In order to successfully run the project, we will need run the script by setting at least the 
following environment variables:

    * `RECIPIENT_EMAIL`: The email address to send the report to.
    * `SENDER_EMAIL`: The email address to send the report from.
    * `SENDER_PASSWORD`: The password for the email address.

This can be done by setting the environment variables in the terminal or adding a few arguments to the script command.

    $ python main.py -re <RECIPIENT_EMAIL> -se <SENDER_EMAIL> -sp <SENDER_PASSWORD>

Additionally, the script will need to be run with the following commands if we want to overwrite the default 
search parameters:

    * `-t`: The search titles to use separated by a comma.
    * `-k`: The search keywords to use separated by a comma.
    * `-sf`: The search sort field defaulted to 'DatePosted'.
    * `-sd`: The search sort order defaulted to 'Descending'.

Paths are automatically set using os. Exports generated by the script are stored in the `export/day` directory at 
the root of the project.

In order to set up a cron job on a GCP server, we would need to start a virtual environment and install our dependencies
using a tool such as miniconda or running our pipenv environment.

First, we would copy python's root directory, which we can find by running the following command:

    $ whihc python

Then we will open a crontab file in your default editor. 

    $ crontab -e

Add the following lines:

    0 0 * * * cd /path/to/interpreter/python script_name.py (+ all the CLI args we wish to add) >> logs.txt

Note that we store the output of the script in the logs.txt file. This is useful for debugging purposes. We are also
running the job every day at midnight by adding two zeros as the first arguments of the cron job.

    0 0 * * *

# Improvements:

With more time I would have added more features to the script. For example:

    * Further reaseach the API functionality to understand why so little results are returned.
    * Extract all variants for each position by further parsing and transforming the API's responses.
    * Use professional project structure and architecture.
    * Add more specific error handling to catch errors accurately.
    * Improve models if we extract more data points by separating relationships into different tables using foreign keys.
    * Add logs for debugging with logger module.
    * Improve email sending security.
    * Add unit and integration tests.
    * Improve CLI experience.


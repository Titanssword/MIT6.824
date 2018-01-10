This is CJ
The project begins at 2018.1.3
until 2018.1.11 i have finished the lab1

fix problem 1:
need password to push to the remote repo everytime
solved:
Open .git/config and find the [remote "origin"] section. Make sure you're using the SSH one:

ssh://git@github.com/username/repo.git

And NOT the https or git one:

https://github.com/username/repo.git
git://github.com/username/repo.git

You can now validate with just the SSH Key instead of the username and password.

[Edit:] If Git complains that 'origin' has already been added, open the .config file and edit the url = "..." part after [remote origin]


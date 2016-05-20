cd $PWD/JobTracker
echo "Compiling......!!"

javac -d bin src/*/*.java

java -cp bin JobTracker.JobTracker_server


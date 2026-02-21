import os

def java_home_location():
    os.environ["JAVA_HOME"] = "/Users/cantaruttim/.sdkman/candidates/java/current"
    os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

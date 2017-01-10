for file in $(find . -name pom.xml); do
 sed -i 's/<version>1.0<\/version>/<version>1.0-SNAPSHOT<\/version>/g' $file;
done

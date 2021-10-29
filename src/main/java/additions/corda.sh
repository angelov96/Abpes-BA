#!/bin/bash

FILE_PATH=E:/cordapp-template-java-latest/workflows/src/main/java/com/template/flows
FILES=$FILE_PATH/*
CONTRACTS=E:/cordapp-template-java-latest/contracts/src/main/java/com/template/contracts
STATES=E:/cordapp-template-java-latest/contracts/src/main/java/com/template/states

PKG_CONTRACTS=E:/cordapp-template-java-latest/contracts/src/main/java
PKG_FLOWS=E:/cordapp-template-java-latest/workflows/src/main/java

CORDA_PATH=E:/cordapp-template-java-latest

JAR_CONTRACTS=E:/cordapp-template-java-latest/contracts/build/libs/contracts-0.1.jar
JAR_FLOWS=E:/cordapp-template-java-latest/workflows/build/libs/workflows-0.1.jar
DEST_PATH=E:/cordapp-template-java-latest/jars

#for %f in (.\*) do @echo ren %f %~nf
#ren *.java *.test

COUNTER=1

FILE_ENDING_PREV=.test
FILE_ENDING_TRGT=.java

for f in $FILES; do
  COUNTER=$((COUNTER + 1))

  if [[ $((COUNTER % 2)) == 0 ]]; then
    FILE_NAME=$(basename "$f" .test)
    FILE_NAME_WITHOUT_FLOW=${FILE_NAME::-4}
    RESULT_CONTRACT=$(ls $CONTRACTS | grep "${FILE_NAME_WITHOUT_FLOW}Contract")
    RESULT_STATE=$(ls $STATES | grep "${FILE_NAME_WITHOUT_FLOW}State")

    #echo "x3 $FILE_NAME"

    #if [[ -n "$RESULT_CONTRACT" ]]; then
    #echo "x1 $RESULT_CONTRACT"
    #fi

    #  if [[ -n "$RESULT_STATE" ]]; then
    #echo "x2 $RESULT_STATE"
    #fi

    if [[ -n "$RESULT_CONTRACT" && -n "$RESULT_STATE" ]]; then

      FLOW_NAME=$(basename "$f" $FILE_ENDING_PREV)
      FLOW_NAME_LOWER=$(echo "${FLOW_NAME}" | awk '{print tolower($0)}')
      FLOW_JAR=$(echo "${FLOW_NAME::-4}"-workflow.jar | awk '{print tolower($0)}')
      CONTRACT_JAR=$(echo "${FLOW_NAME::-4}"-contract.jar | awk '{print tolower($0)}')

      mkdir -p $PKG_CONTRACTS/com/$FLOW_NAME_LOWER/contracts
      mkdir -p $PKG_CONTRACTS/com/$FLOW_NAME_LOWER/states
      mkdir -p $PKG_FLOWS/com/$FLOW_NAME_LOWER/flows

      cd $CONTRACTS || exit
      mv "$RESULT_CONTRACT" $PKG_CONTRACTS/com/$FLOW_NAME_LOWER/contracts/$(basename "$RESULT_CONTRACT" $FILE_ENDING_PREV)$FILE_ENDING_TRGT
      cd $STATES || exit
      mv "$RESULT_STATE" $PKG_CONTRACTS/com/$FLOW_NAME_LOWER/states/$(basename "$RESULT_STATE" $FILE_ENDING_PREV)$FILE_ENDING_TRGT
      cd $FILE_PATH || exit
      mv $(basename "$f") $PKG_FLOWS/com/$FLOW_NAME_LOWER/flows/$(basename "$f" $FILE_ENDING_PREV)$FILE_ENDING_TRGT
      mv ${FLOW_NAME}Responder$FILE_ENDING_PREV $PKG_FLOWS/com/$FLOW_NAME_LOWER/flows/${FLOW_NAME}Responder$FILE_ENDING_TRGT

      PKG_TO_REPLACE="com.template.";
      PKG_REPLACED_BY="com.${FLOW_NAME_LOWER}.";

      sed -i "s/$PKG_TO_REPLACE/$PKG_REPLACED_BY/g" $PKG_CONTRACTS/com/$FLOW_NAME_LOWER/contracts/$(basename "$RESULT_CONTRACT" $FILE_ENDING_PREV)$FILE_ENDING_TRGT
      sed -i "s/$PKG_TO_REPLACE/$PKG_REPLACED_BY/g" $PKG_CONTRACTS/com/$FLOW_NAME_LOWER/states/$(basename "$RESULT_STATE" $FILE_ENDING_PREV)$FILE_ENDING_TRGT
      sed -i "s/$PKG_TO_REPLACE/$PKG_REPLACED_BY/g" $PKG_FLOWS/com/$FLOW_NAME_LOWER/flows/$(basename "$f" $FILE_ENDING_PREV)$FILE_ENDING_TRGT
      sed -i "s/$PKG_TO_REPLACE/$PKG_REPLACED_BY/g" $PKG_FLOWS/com/$FLOW_NAME_LOWER/flows/${FLOW_NAME}Responder$FILE_ENDING_TRGT

      cd $CORDA_PATH || exit

      chmod +x gradlew.bat
      ./gradlew.bat clean -q --no-build-cache
      ./gradlew.bat dependencies -q
      ./gradlew.bat installQuasar -q
      ./gradlew.bat build -q
      ./gradlew.bat deployNodes

      cp $JAR_FLOWS $DEST_PATH/"$FLOW_JAR"
      cp $JAR_CONTRACTS $DEST_PATH/"$CONTRACT_JAR"

      sed -i "s/$PKG_REPLACED_BY/$PKG_TO_REPLACE/g" $PKG_CONTRACTS/com/$FLOW_NAME_LOWER/contracts/$(basename "$RESULT_CONTRACT" $FILE_ENDING_PREV)$FILE_ENDING_TRGT
      sed -i "s/$PKG_REPLACED_BY/$PKG_TO_REPLACE/g" $PKG_CONTRACTS/com/$FLOW_NAME_LOWER/states/$(basename "$RESULT_STATE" $FILE_ENDING_PREV)$FILE_ENDING_TRGT
      sed -i "s/$PKG_REPLACED_BY/$PKG_TO_REPLACE/g" $PKG_FLOWS/com/$FLOW_NAME_LOWER/flows/$(basename "$f" $FILE_ENDING_PREV)$FILE_ENDING_TRGT
      sed -i "s/$PKG_REPLACED_BY/$PKG_TO_REPLACE/g" $PKG_FLOWS/com/$FLOW_NAME_LOWER/flows/${FLOW_NAME}Responder$FILE_ENDING_TRGT

      cd $CONTRACTS || exit
      mv $PKG_CONTRACTS/com/$FLOW_NAME_LOWER/contracts/$(basename "$RESULT_CONTRACT" $FILE_ENDING_PREV)$FILE_ENDING_TRGT $(basename "$RESULT_CONTRACT" $FILE_ENDING_TRGT)
      cd $STATES || exit
      mv $PKG_CONTRACTS/com/$FLOW_NAME_LOWER/states/$(basename "$RESULT_STATE" $FILE_ENDING_PREV)$FILE_ENDING_TRGT $(basename "$RESULT_STATE" $FILE_ENDING_TRGT)
      cd $FILE_PATH || exit
      mv $PKG_FLOWS/com/$FLOW_NAME_LOWER/flows/$(basename "$f" $FILE_ENDING_PREV)$FILE_ENDING_TRGT $(basename "$f" $FILE_ENDING_TRGT)
      mv $PKG_FLOWS/com/$FLOW_NAME_LOWER/flows/${FLOW_NAME}Responder$FILE_ENDING_TRGT ${FLOW_NAME}Responder$FILE_ENDING_PREV

      rm -r $PKG_CONTRACTS/com/$FLOW_NAME_LOWER
      rm -r $PKG_FLOWS/com/$FLOW_NAME_LOWER

    fi

  fi

done

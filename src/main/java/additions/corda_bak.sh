#!/bin/bash

FILE_PATH=E:/cordapp-template-java/workflows/src/main/java/com/template/flows
FILES=$FILE_PATH/*
CONTRACTS=E:/cordapp-template-java/contracts/src/main/java/com/template/contracts
STATES=E:/cordapp-template-java/contracts/src/main/java/com/template/states

PKG=STATES=E:/cordapp-template-java/contracts/src/main/java/

CORDA_PATH=E:/cordapp-template-java

JAR_CONTRACTS=E:/cordapp-template-java/contracts/build/libs/contracts-0.1.jar
JAR_FLOWS=E:/cordapp-template-java/workflows/build/libs/workflows-0.1.jar
DEST_PATH=E:/cordapp-template-java/jars

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

      mkdir $CONTRACTS/$FLOW_NAME_LOWER
      mkdir $STATES/$FLOW_NAME_LOWER
      mkdir $FILE_PATH/$FLOW_NAME_LOWER

      cd $CONTRACTS || exit
      mv "$RESULT_CONTRACT" $CONTRACTS/$FLOW_NAME_LOWER/$(basename "$RESULT_CONTRACT" $FILE_ENDING_PREV)$FILE_ENDING_TRGT
      cd $STATES || exit
      mv "$RESULT_STATE" $STATES/$FLOW_NAME_LOWER/$(basename "$RESULT_STATE" $FILE_ENDING_PREV)$FILE_ENDING_TRGT
      cd $FILE_PATH || exit
      mv $(basename "$f") $FILE_PATH/$FLOW_NAME_LOWER/$(basename "$f" $FILE_ENDING_PREV)$FILE_ENDING_TRGT

      cd $CORDA_PATH || exit

      chmod +x gradlew.bat
      ./gradlew.bat deployNodes

      cp $JAR_FLOWS $DEST_PATH/"$FLOW_JAR"
      cp $JAR_CONTRACTS $DEST_PATH/"$CONTRACT_JAR"

      cd $CONTRACTS || exit
      mv $CONTRACTS/$FLOW_NAME_LOWER/$(basename "$RESULT_CONTRACT" $FILE_ENDING_PREV)$FILE_ENDING_TRGT $(basename "$RESULT_CONTRACT" $FILE_ENDING_TRGT)
      cd $STATES || exit
      mv $STATES/$FLOW_NAME_LOWER/$(basename "$RESULT_STATE" $FILE_ENDING_PREV)$FILE_ENDING_TRGT $(basename "$RESULT_STATE" $FILE_ENDING_TRGT)
      cd $FILE_PATH || exit
      mv $FILE_PATH/$FLOW_NAME_LOWER/$(basename "$f" $FILE_ENDING_PREV)$FILE_ENDING_TRGT $(basename "$f" $FILE_ENDING_TRGT)

exit 0

    fi

  fi

done

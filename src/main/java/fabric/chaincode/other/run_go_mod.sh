for d in */; do var=$d"go" && cd $d"go" && "$(find /c/Users/*/sdk/ -name go.exe)" mod init $var && cd -; done

var=${PWD##*/}/go && cd go && "$(find /c/Users/*/sdk/ -name go.exe)" mod init $var && cd -

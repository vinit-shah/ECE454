exception IllegalArgument {
  1: string message;
}

service BcryptService {
 list<string> hashPassword (1: list<string> password, 2: i16 logRounds) throws (1: IllegalArgument e);
 list<bool> checkPassword (1: list<string> password, 2: list<string> hash) throws (1: IllegalArgument e);
 list<string> hashPasswordCompute (1: list<string> password, 2: i16 logRounds) throws (1: IllegalArgument e);
 list<bool> checkPasswordCompute (1: list<string> password, 2: list<string> hash) throws (1: IllegalArgument e);
 void registerBENode(1: string hostname, 2: i32 portNumber) throws (1:IllegalArgument e);
}

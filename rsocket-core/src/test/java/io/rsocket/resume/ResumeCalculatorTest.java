/// *
// * Copyright 2015-2019 the original author or authors.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
// package io.rsocket.resume;
//
// import org.junit.jupiter.api.Assertions;
// import org.junit.jupiter.api.BeforeEach;
// import org.junit.jupiter.api.Test;
//
// public class ResumeCalculatorTest {
//
//  @BeforeEach
//  void setUp() {}
//
//  @Test
//  void clientResumeSuccess() {
//    long position = ResumableDuplexConnection.calculateRemoteImpliedPos(1, 42, -1, 3);
//    Assertions.assertEquals(3, position);
//  }
//
//  @Test
//  void clientResumeError() {
//    long position = ResumableDuplexConnection.calculateRemoteImpliedPos(4, 42, -1, 3);
//    Assertions.assertEquals(-1, position);
//  }
//
//  @Test
//  void serverResumeSuccess() {
//    long position = ResumableDuplexConnection.calculateRemoteImpliedPos(1, 42, 4, 23);
//    Assertions.assertEquals(23, position);
//  }
//
//  @Test
//  void serverResumeErrorClientState() {
//    long position = ResumableDuplexConnection.calculateRemoteImpliedPos(1, 3, 4, 23);
//    Assertions.assertEquals(-1, position);
//  }
//
//  @Test
//  void serverResumeErrorServerState() {
//    long position = ResumableDuplexConnection.calculateRemoteImpliedPos(4, 42, 4, 1);
//    Assertions.assertEquals(-1, position);
//  }
// }

/**
 * Copyright 2015 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.reactivesocket.exceptions;

import io.reactivesocket.Frame;
import io.reactivesocket.FrameType;

public class SetupException extends Exception {
	private static final long serialVersionUID = 1851832033885686474L;

	public static enum SetupErrorCode {
		INVALID_SETUP(0x0001), UNSUPPORTED(0x0010), REJECTED(0x100);

		private final int code;

		private SetupErrorCode(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}

	}

	private final SetupErrorCode code;

	public SetupException(Frame frame) {
		super();
		Frame.ensureFrameType(FrameType.SETUP_ERROR, frame);
		int errorCode = Frame.SetupError.errorCode(frame);
		if(errorCode == 0x0001) {
			this.code = SetupErrorCode.INVALID_SETUP;
		} else if(errorCode == 0x0010) {
			this.code = SetupErrorCode.UNSUPPORTED;
		} else if(errorCode == 0x100) {
			this.code = SetupErrorCode.REJECTED;
		} else {
			throw new IllegalArgumentException("Invalid SetupErrorCode: " + errorCode);
		}
	}
	
	public SetupException(SetupErrorCode code) {
		super();
		this.code = code;
	}

	public SetupException(SetupErrorCode code, Throwable t) {
		super(t);
		this.code = code;
	}
	
	public SetupException(SetupErrorCode code, String message) {
		super(message);
		this.code = code;
	}

	public SetupErrorCode getErrorCode() {
		return code;
	}

}

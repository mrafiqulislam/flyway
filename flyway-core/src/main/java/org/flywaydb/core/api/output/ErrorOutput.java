/*
 * Copyright (C) Red Gate Software Ltd 2010-2021
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.api.output;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import org.flywaydb.core.api.ErrorCode;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.command.DbMigrate;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class ErrorOutput implements OperationResult {

    @AllArgsConstructor(access = AccessLevel.PACKAGE)
    public static class ErrorOutputItem {
        public ErrorCode errorCode;
        public String message;
        public String stackTrace;
    }

    public ErrorOutputItem error;

    public ErrorOutput(ErrorCode errorCode, String message, String stackTrace) {
        this.error = new ErrorOutputItem(errorCode, message, stackTrace);
    }

    public static ErrorOutput fromException(Exception exception) {
        String message = exception.getMessage();

        if (exception instanceof FlywayException) {
            FlywayException flywayException = (FlywayException)exception;

            return new ErrorOutput(
                    flywayException.getErrorCode(),
                    message == null ? "Error occurred" : message,
                    null);
        }

        return new ErrorOutput(
                ErrorCode.FAULT,
                message == null ? "Fault occurred" : message,
                getStackTrace(exception));
    }

    public static MigrateErrorResult fromMigrateException(DbMigrate.FlywayMigrateException exception) {
        return exception.getErrorResult();
    }

    private static String getStackTrace(Exception exception) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream printStream;

        try {
            printStream = new PrintStream(output, true, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            return "";
        }

        exception.printStackTrace(printStream);

        return new String(output.toByteArray(), StandardCharsets.UTF_8);
    }
}
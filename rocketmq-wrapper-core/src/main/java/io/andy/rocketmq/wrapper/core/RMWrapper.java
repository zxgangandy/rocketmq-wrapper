/**
 * MIT License
 *
 * Copyright (c) 2020 andy zhao
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package io.andy.rocketmq.wrapper.core;

import java.lang.reflect.Constructor;

public class RMWrapper {

    private static volatile RMWrapper INSTANCE;

    public static <T> T with(Class<T> clazz ) {
        return getDefault().build(clazz);
    }

    private static RMWrapper getDefault() {
        if ( null == INSTANCE) {
            synchronized (RMWrapper.class) {
                if (null == INSTANCE) {
                    INSTANCE = new RMWrapper();
                }
            }
        }

        return INSTANCE;
    }

    public  <T> T build(Class<T> clazz)  {
        try {
            Constructor<T> constructor = clazz.getConstructor();
            return constructor.newInstance();
        } catch (Throwable e) {
            e.printStackTrace();
            throw new IllegalStateException(String.format("Start (%s) failed!", clazz.getSimpleName()));
        }
    }
}

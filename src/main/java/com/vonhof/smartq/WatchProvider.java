package com.vonhof.smartq;

/**
 * Simple class for providing a overridable replacement for System.currentTimeMillis().
 * Used for predictable time calculations in tests.
 */
class WatchProvider {

    private static long currentTime = -1;

    public static long currentTime() {
       return currentTime(-1);
    }

    public static long currentTime(long time) {
        if (time > -1 ) {
            currentTime = time;
        }

        if (currentTime > -1) {
            return currentTime;
        }

        return System.currentTimeMillis();
    }

    public static void appendTime(long time) {
        if (currentTime < 0) {
            currentTime = 0;
        }

        currentTime += time;
    }
}

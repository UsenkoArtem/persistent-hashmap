package dev.evo.persistent.hashmap.straight

import dev.evo.persistent.util.withTempDir

import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.ReentrantLock

class StraightHashMapROEnvConcurrencyTests : FunSpec() {
    companion object {
        private const val THREADS = 16
        private const val ROUNDS = 200
    }

    private fun StraightHashMapROEnv<*, *, *>.envLock(): ReentrantLock {
        val field = StraightHashMapROEnv::class.java.getDeclaredField("lock")
        field.isAccessible = true
        return field.get(this) as ReentrantLock
    }

    init {
        // Reproduces a hang seen in production: when several threads call
        // getCurrentMap() on a freshly opened read-only environment, the threads
        // that waited for the lock while the first one opened the file
        // used to leave the lock locked, so the threads queued after them
        // blocked in lock.lock() forever.
        test("ro env: concurrent first getCurrentMap releases the lock") {
            withTempDir { tmpDir ->
                StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                    .open(tmpDir)
                    .use { env ->
                        env.openMap().use { map ->
                            map.put(1, 1.5F) shouldBe PutResult.OK
                        }
                    }

                val executor = Executors.newFixedThreadPool(THREADS)
                try {
                    repeat(ROUNDS) { round ->
                        StraightHashMapEnv.Builder(StraightHashMapType_Int_Float)
                            .openReadOnly(tmpDir)
                            .use { roEnv ->
                                val barrier = CyclicBarrier(THREADS)
                                val futures = (1..THREADS).map {
                                    executor.submit {
                                        barrier.await()
                                        roEnv.getCurrentMap().use { roMap ->
                                            roMap.get(1, 0F) shouldBe 1.5F
                                        }
                                    }
                                }
                                futures.forEach { future ->
                                    try {
                                        future.get(10, TimeUnit.SECONDS)
                                    } catch (e: TimeoutException) {
                                        throw AssertionError(
                                            "Round $round: getCurrentMap() hung, " +
                                                "lock=${roEnv.envLock()}",
                                            e
                                        )
                                    }
                                }
                                val lock = roEnv.envLock()
                                if (lock.isLocked) {
                                    throw AssertionError("Round $round: lock was leaked: $lock")
                                }
                            }
                    }
                } finally {
                    executor.shutdownNow()
                }
            }
        }
    }
}

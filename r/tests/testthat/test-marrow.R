# Title     : TODO
# Objective : TODO
# Created by: adorr
# Created on: 17/01/2020
context("marrowApi")

    batch <- record_batch(a=c(5, 4, 3, 2, 1), b=c(10, 20, 33, 40, 50))

test_that("add_index", {
    actual <- add_index(batch, "a")
    expected <- record_batch(__marray_index=c(4, 3, 2, 1, 0), a=c(5, 4, 3, 2, 1), b=c(10, 20, 30, 40, 50))
    expect_data_frame(actual$ad.data.frame(), expected.as.data.frame())
})

test_that("sort", {
    actual <- sort(batch, "a")
    expected <- record_batch(a=c(1, 2, 3, 4, 5), b=c(50, 40, 30, 20, 10))
    expect_data_frame(actual$ad.data.frame(), expected.as.data.frame())
})

test_that("merge", {
    batch1 <- sort(record_batch(a=c(1, 3. 4, 4), b=c(10, 20, 30, 40)), "a")
    batch2 <- sort(record_batch(a=c(1, 1, 2, 4), c=c(11, 21, 31, 41)), "b")
    actual <- merge(batch1, batch2, "a")
    expected <- record_batch(a=c(1, 2, 3, 4, 5), b=c(50, 40, 30, 20, 10))
    expect_data_frame(actual$ad.data.frame(), expected.as.data.frame())
})

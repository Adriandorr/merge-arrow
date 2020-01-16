//
// Created by adorr on 16/01/2020.
//

#include "marrow/api.h"
#include "batch_maker.h"
#include <gtest/gtest.h>
#include "test_helpers.h"

class TestApi : public testing::Test {

};

TEST_F(TestApi, TestAddIndex) {
    auto batch = BatchMaker()
            .add_string_array<>("a", {"1", "2", "1", "2", "0"})
            .add_array<>("b", {100, 150, 99, 200, 1000})
            .add_array<>("c", {2, 3, 1, 4, 0})
            .record_batch();

    std::shared_ptr<arrow::Array> index;
    auto actual = marrow::add_index(batch, {"a", "b"});

    auto expected = BatchMaker()
            .add_meta_data("__marrow_index", "a,b")
            .template add_array<arrow::Int8Type>("__marrow_index", {4, 2, 0, 1, 3}, 99)
            .add_string_array<>("a", {"1", "2", "1", "2", "0"})
            .add_array<>("b", {100, 150, 99, 200, 1000})
            .add_array<>("c", {2, 3, 1, 4, 0})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}


TEST_F(TestApi, TestSort) {
    auto batch = BatchMaker()
            .add_string_array<>("a", {"1", "2", "1", "2", "0"})
            .add_array<>("b", {100, 150, 99, 200, 1000})
            .add_array<>("c", {2, 3, 1, 4, 0})
            .record_batch();

    std::shared_ptr<arrow::Array> index;
    auto actual = marrow::sort(batch, {"a", "b"});

    auto expected = BatchMaker()
            .add_meta_data("__marrow_index", "a,b")
            .add_string_array<>("a", {"0", "1", "1", "2", "2"})
            .add_array<>("b", {1000, 99, 100, 150, 200})
            .add_array<>("c", {0, 1, 2, 3, 4})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

TEST_F(TestApi, TestLeft) {
    auto batch1 = BatchMaker()
            .add_array<>("a", {1, 1, 2, 3, 5})
            .add_array<>("b", {11, 12, 21, 31, 51})
            .record_batch();

    auto batch2 = BatchMaker()
            .add_array<>("a", {1, 2, 4, 5, 5})
            .add_array<>("c", {11, 21, 41, 51, 52})
            .record_batch();

    auto actual = marrow::merge(batch1, batch2, {"a"}, "left", "_right"); //h

    auto expected = BatchMaker()
            .add_array<>("a", {1, 1, 2, 3, 5, 5})
            .add_array<>("b", {11, 12, 21, 31, 51, 51})
            .add_array<>("c_right", {11, 11, 21, 0, 51, 52})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

TEST_F(TestApi, TestInner) {
    auto batch1 = BatchMaker()
            .add_array<>("a", {1, 1, 2, 3, 5}, 99)
            .template add_array<>("b", {11, 12, 21, 31, 51})
            .record_batch();

    auto batch2 = BatchMaker()
            .add_array<>("a", {1, 2, 4, 5, 5}, 99)
            .template add_array<>("c", {11, 21, 41, 51, 52})
            .record_batch();

    auto actual = marrow::merge(batch1, batch2, {"a"}, "inner", "_right"); //h

    auto expected = BatchMaker()
            .add_array<>("a", {1, 1, 2, 5, 5}, 99)
            .template add_array<>("b", {11, 12, 21, 51, 51})
            .template add_array<>("c", {11, 11, 21, 51, 52})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

TEST_F(TestApi, TestOuter) {
    auto batch1 = BatchMaker()
            .add_array<>("a", {1, 1, 2, 3, 5})
            .add_array<>("b", {11, 12, 21, 31, 51})
            .record_batch();

    auto batch2 = BatchMaker()
            .add_array<>("a", {1, 2, 4, 5, 5})
            .add_array<>("c", {11, 21, 41, 51, 52})
            .record_batch();

    auto actual = marrow::merge(batch1, batch2, {"a"}, "outer", ""); //h

    auto expected = BatchMaker()
            .add_array<>("a", {1, 1, 2, 3, 4, 5, 5})
            .add_array<>("b", {11, 12, 21, 31, 0, 51, 51})
            .add_array<>("c_right", {11, 11, 21, 0, 41, 51, 52})
            .record_batch();
    SCOPED_TRACE(compare_msg(actual, expected));
    ASSERT_TRUE(actual->Equals(*expected));
}

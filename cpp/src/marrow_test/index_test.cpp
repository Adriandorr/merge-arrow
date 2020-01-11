//
// Created by adorr on 11/01/2020.
//

#include "marrow/index.h"
#include "gtest/gtest.h"
#include "batch_maker.h"
#include "test_helpers.h"

class TestIndex : public testing::Test {

};

TEST_F(TestIndex, Test1) {
    auto batch = BatchMaker()
            .add_string_array<>("a", {"1", "2", "1", "2", "0"})
            .add_array<>("b", {100, 150, 99, 200, 1000})
            .add_array<>("c", {2, 3, 1, 4, 0})
            .record_batch();

    std::shared_ptr<arrow::Array> index;
    ASSERT_STATUS_OK(marrow::make_index(batch, {"a", "b"}, &index));
    SCOPED_TRACE("index: " + index->ToString());
    auto expected = BatchMaker().add_array<arrow::Int8Type>("", {4, 2, 0, 1, 3}, 99).array();
    SCOPED_TRACE("expected: " + expected->ToString());
    ASSERT_TRUE(index->Equals(*expected));
}
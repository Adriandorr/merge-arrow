//
// Created by adorr on 11/01/2020.
//

#ifndef MARROW_TEST_HELPERS_H
#define MARROW_TEST_HELPERS_H

#define ASSERT_STATUS_OK(s) \
{                       \
    auto status = (s);  \
    SCOPED_TRACE(status.ToString());    \
    ASSERT_TRUE(s.ok());    \
}

#endif //MARROW_TEST_HELPERS_H

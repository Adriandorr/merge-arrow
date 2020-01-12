//
// Created by adorr on 12/01/2020.
//

#ifndef MARROW_ARROW_THROW_H
#define MARROW_ARROW_THROW_H

#define ARROW_THROW_NOT_OK(s)   \
{                                                       \
    arrow::Status status = (s);                                  \
    if (!status.ok()) {                                 \
        throw std::runtime_error(status.ToString());    \
    }                                                   \
}

#endif //MARROW_ARROW_THROW_H

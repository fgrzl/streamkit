## 🧪 Testing Guidelines

- All code must be covered with meaningful tests.
- Use Go’s standard `testing` package with `testify/assert` for assertions and `testify/require` to short circuit when arrange sections fail.
- Each test should only test one thing.
- Use table driven tests when a test behavior is operating on variations of the inputs
- Write tests in **behavioral style**, using names like:

  ```go
  func TestShouldReturnErrorWhenUserIsInvalid(t *testing.T)
  func TestShouldStoreResultGivenValidInput(t *testing.T)
  ```

- Inside tests, follow this structure with clear comments:

  ```go
  // Arrange
  require.NoError(t, err)
  ...

  // Act
  ...

  // Assert
  assert.Equal(t, expected, actual)

  ```

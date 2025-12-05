/*
  Add a new column containing the rescaled ratings mapped from the original scale 1 to 5
  into a zero-centered integer scale from -4 to 4, where:

    - Original rating 3 maps to 0 (neutral midpoint),
    - 1 maps to -4 (lowest rating),
    - 5 maps to +4 (highest rating).

  Rationale:
  - This linear transformation centers the ratings around zero,
    facilitating easier interpretation of positive and negative deviations.
  - Preserves relative distances and order of ratings but normalizes the scale.
  - Helps with statistical analyses and outlier detection by removing absolute scale bias.
  - Enables more intuitive visualization and comparison of user preferences or wine quality.
  
  The transformation formula used is:
  
      rescaled_rating = (rating - 3) * 2

  This converts half-step increments into consecutive integers on the new scale.

  Note:
  - This approach assumes the original rating scale is continuous with half-step increments.
  - The rescaled column stores integer values for simplicity and clarity.
*/

-- Step 1: Add a new column to store the rescaled rating (integer type)
ALTER TABLE ratings
ADD COLUMN rescaled_rating INTEGER;

-- Step 2: Populate the new column with the linear transformation
UPDATE ratings
SET rescaled_rating = (rating - 3) * 2;
